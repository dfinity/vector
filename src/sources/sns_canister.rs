//! Generalized HTTP client source.
//! Calls an endpoint at an interval, decoding the HTTP responses into events.

use crate::config::schema::Definition;
use crate::http::HttpClient;
use crate::internal_events::{JournaldCheckpointFileOpenError, StreamClosedError};
use crate::sources::journald::{SharedCheckpointer, StatefulCheckpointer};
use crate::SourceSender;
use crate::{
    config::{SourceConfig, SourceContext},
    sources,
};
use hyper::header::ACCEPT;
use hyper::http::StatusCode;
use metrics::counter;
use std::net::AddrParseError;
use std::path::PathBuf;
use std::time::Duration;
use vector_common::internal_event::{error_stage, error_type, InternalEvent};
use vector_common::shutdown::ShutdownSignal;
use vector_config::configurable_component;
use vector_core::config::proxy::ProxyConfig;
use vector_core::config::{log_schema, DataType, LogNamespace, SourceOutput};
use vector_core::event::LogEvent;
use vector_core::tls::{TlsConfig, TlsSettings};

const CHECKPOINT_FILENAME: &str = "checkpoint.txt";
const BATCH_SIZE: u64 = 32;
const SLEEP_DURATION: u64 = 1;

/// Configuration for the `sns_canister` source.
#[configurable_component(source("sns_canister", "Collect logs from sns canisters of Dfinity"))]
#[derive(Clone, Debug, Default)]
pub struct SnsCanisterConfig {
    /// Canister url to collect logs from. Only base url should be specified.
    /// Example: "http://5s2ji-faaaa-aaaaa-qaaaq-cai.ic0.app"
    pub endpoint: String,

    /// The directory used to persist checkpoints positions.
    ///
    /// By default, the global `data_dir` option is used. Make sure the running user has write permissions to this directory.
    pub data_dir: Option<PathBuf>,

    /// The sleep duration in seconds between requests
    ///
    /// By default, 1 second is used.
    pub sleep_duration: Option<u64>,
}

impl_generate_config_from_default!(SnsCanisterConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "sns_canister")]
impl SourceConfig for SnsCanisterConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;

        let sleep = self.sleep_duration.unwrap_or(SLEEP_DURATION);

        let mut checkpoint_path = data_dir;
        checkpoint_path.push(CHECKPOINT_FILENAME);
        let proxy = ProxyConfig::from_env();
        let default = TlsConfig::default();
        let tls = TlsSettings::from_options(&Some(default))?;
        Ok(Box::pin(
            SnsCanisterSource {
                endpoint: self.endpoint.clone(),
                out: cx.out,
                checkpoint_path,
                client: HttpClient::new(tls, &proxy)?,
                sleep,
            }
            .run_shutdown(cx.shutdown),
        ))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_logs(DataType::Log, Definition::any())]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

/// Captures the configuration options required to decode the incoming requests into events.
#[derive(Clone)]
struct SnsCanisterSource {
    endpoint: String,
    out: SourceSender,
    checkpoint_path: PathBuf,
    client: HttpClient<hyper::Body>,
    sleep: u64,
}

impl SnsCanisterSource {
    async fn run_shutdown(self, shutdown: ShutdownSignal) -> Result<(), ()> {
        let checkpointer = StatefulCheckpointer::new(self.checkpoint_path.clone())
            .await
            .map_err(|error| {
                emit!(JournaldCheckpointFileOpenError {
                    error,
                    path: self
                        .checkpoint_path
                        .to_str()
                        .unwrap_or("unknown")
                        .to_string(),
                });
            })?;

        let checkpointer = SharedCheckpointer::new(checkpointer);

        self.run(shutdown, checkpointer).await?;

        Ok(())
    }

    async fn run(
        mut self,
        mut shutdown: ShutdownSignal,
        checkpointer: SharedCheckpointer,
    ) -> Result<(), ()> {
        let cursor = checkpointer.lock().await.cursor.clone();

        let mut last_cursor = match cursor {
            Some(val) => val,
            None => "".to_string(),
        };
        let mut batch = BATCH_SIZE;

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = tokio::time::sleep(Duration::from_secs(self.sleep)) => {},
            }
            let url = match last_cursor == "".to_string() {
                true => format!("{}/logs", self.endpoint),
                false => format!("{}/logs?time={}", self.endpoint, last_cursor),
            };
            let req = hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri(&url)
                .header(ACCEPT, "application/json".to_string())
                .body(hyper::Body::empty())
                .unwrap();

            let body = tokio::select! {
                biased;
                _ = &mut shutdown => break,
                result = self.client.send(req) => match result {
                    Ok(resp) => {
                        match resp.status() {
                            StatusCode::OK => {
                                resp.into_body()
                            }
                            _ => continue,
                        }
                    },
                    Err(e) => {
                        warn!("Couldn't reach canister... Backing off. Error was: {}", e);
                        tokio::select! {
                            biased;
                            _ = &mut shutdown => break,
                            _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                        }
                    }
                }
            };

            let body_bytes = tokio::select! {
                biased;
                _ = &mut shutdown => break,
                result = hyper::body::to_bytes(body) => result.map_err(|error| emit!(SnsCanisterReadError { error }))?
            };

            let body_string = String::from_utf8(body_bytes.to_vec())
                .map_err(|error| emit!(SnsCanisterJsonError { error }))?;

            let sns_logs =
                match serde_json::from_str::<SnsLogRecordsAggregated>(body_string.as_str()) {
                    Ok(sns_logs) => sns_logs,
                    Err(error) => {
                        emit!(SnsSerdeJsonError {
                            error,
                            canister_id: self.endpoint.to_string()
                        });
                        warn!("Couldn't parse body:\n{}", body_string.as_str());
                        continue;
                    }
                };

            if sns_logs.entries.len() == 0 {
                continue;
            }

            for log in &sns_logs.entries {
                self.out
                    .send_event(log.into_event())
                    .await
                    .map_err(|error| emit!(StreamClosedError { error, count: 1 }))?
            }

            last_cursor = match sns_logs.last() {
                Some(log) => log.timestamp.to_string(),
                None => last_cursor,
            };
            batch -= 1;
            if batch == 0 {
                checkpointer.lock().await.set(last_cursor.clone()).await;
                batch = BATCH_SIZE;
            }
        }

        checkpointer.lock().await.set(last_cursor).await;
        Ok(())
    }
}

#[derive(serde::Deserialize, Clone)]
struct SnsLogRecordsAggregated {
    entries: Vec<SnsLogRecord>,
}

impl SnsLogRecordsAggregated {
    fn last(&self) -> Option<SnsLogRecord> {
        self.entries.last().cloned()
    }
}

#[derive(serde::Deserialize, Clone)]
struct SnsLogRecord {
    timestamp: u64,
    severity: String,
    file: String,
    line: u64,
    message: String,
}

impl SnsLogRecord {
    fn into_event(&self) -> LogEvent {
        let mut log_event = LogEvent::default();
        log_event.insert("timestamp", self.timestamp.clone());
        log_event.insert(log_schema().message_key(), self.message.clone());
        log_event.insert("severity", self.severity.clone());
        log_event.insert("file", self.file.clone());
        log_event.insert("line", self.line);

        log_event
    }
}

#[derive(Debug)]
pub struct SnsCanisterParseUrlError {
    pub error: AddrParseError,
}

impl InternalEvent for SnsCanisterParseUrlError {
    fn emit(self) {
        error!(message = "Unable to parse sns endpoint",
            error = %self.error,
            error_type = error_type::IO_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        counter!(
            "component_errors_total", 1,
            "stage" => error_stage::RECEIVING,
            "error_type" => error_type::IO_FAILED
        );
    }
}

#[derive(Debug)]
pub struct SnsCanisterReadError {
    pub error: hyper::Error,
}

impl InternalEvent for SnsCanisterReadError {
    fn emit(self) {
        error!(message = "Unable to read from endpoint",
            error = %self.error,
            error_type = error_type::IO_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        counter!(
            "component_errors_total", 1,
            "stage" => error_stage::RECEIVING,
            "error_type" => error_type::IO_FAILED,
        );
    }
}

#[derive(Debug)]
pub struct SnsCanisterJsonError {
    pub error: std::string::FromUtf8Error,
}

impl InternalEvent for SnsCanisterJsonError {
    fn emit(self) {
        error!(message = "Unable to parse into json",
            error = %self.error,
            error_type = error_type::IO_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        counter!(
            "component_errors_total", 1,
            "stage" => error_stage::RECEIVING,
            "error_type" => error_type::IO_FAILED,
        );
    }
}

pub struct SnsSerdeJsonError {
    pub error: serde_json::Error,
    pub canister_id: String,
}

impl InternalEvent for SnsSerdeJsonError {
    fn emit(self) {
        error!(message = "Serde failed for sns log entries",
            error = %self.error,
            error_type = error_type::IO_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true,
        );
        counter!(
            "component_errors_total", 1,
            "stage" => error_stage::RECEIVING,
            "error_type" => error_type::IO_FAILED,
            "canister_id" => self.canister_id
        );
    }
}
