//! Generalized HTTP client source.
//! Calls an endpoint at an interval, decoding the HTTP responses into events.

use crate::config::schema::Definition;
use crate::internal_events::{JournaldCheckpointFileOpenError, StreamClosedError};
use crate::sources::journald::{SharedCheckpointer, StatefulCheckpointer};
use crate::SourceSender;
use crate::{
    config::{SourceConfig, SourceContext},
    sources,
};
use hyper::client::HttpConnector;
use hyper::header::{ACCEPT, RANGE};
use hyper::Client;
use metrics::counter;
use std::net::SocketAddr;
use std::net::{AddrParseError, IpAddr};
use std::path::PathBuf;
use std::time::Duration;
use vector_common::internal_event::{error_stage, error_type, InternalEvent};
use vector_common::shutdown::ShutdownSignal;
use vector_config::configurable_component;
use vector_core::config::{DataType, LogNamespace, SourceOutput};
use vector_core::event::LogEvent;

const CHECKPOINT_FILENAME: &str = "checkpoint.txt";
const CURSOR: &str = "__CURSOR";
const BATCH_SIZE: u64 = 32;

/// Configuration for the `systemd_journal_gatewayd` source.
#[configurable_component(source("systemd_journal_gatewayd"))]
#[derive(Clone, Debug, Default)]
pub struct SystemdJournalGatewaydConfig {
    /// Endpoint to collect events from. The full path must be specified.
    /// Example: "::1"
    pub endpoint: String,

    /// The directory used to persist file checkpoint positions.
    ///
    /// By default, the global `data_dir` option is used. Make sure the running user has write permissions to this directory.
    pub data_dir: Option<PathBuf>,

    /// The amount of logs to fetch before persisting the cursor
    ///
    /// By default 32
    pub batch_size: Option<u64>,
}

impl_generate_config_from_default!(SystemdJournalGatewaydConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "systemd_journal_gatewayd")]
impl SourceConfig for SystemdJournalGatewaydConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;

        let batch_size = self.batch_size.unwrap_or(BATCH_SIZE);

        let mut checkpoint_path = data_dir;
        checkpoint_path.push(CHECKPOINT_FILENAME);

        Ok(Box::pin(
            SystemdJournalGatewaydSource {
                endpoint: self.endpoint.clone(),
                out: cx.out,
                checkpoint_path,
                batch_size,
                client: Client::builder().pool_max_idle_per_host(0).build_http(),
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
struct SystemdJournalGatewaydSource {
    endpoint: String,
    out: SourceSender,
    checkpoint_path: PathBuf,
    batch_size: u64,
    client: Client<HttpConnector>,
}

impl SystemdJournalGatewaydSource {
    async fn run_shutdown(self, shutdown: ShutdownSignal) -> Result<(), ()> {
        counter!("sources_started", 1);
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
        let parsed_ip = self.endpoint.parse().map_err(|error| {
            emit!(SystemdJournalGatewaydParseIpError { error });
        })?;
        let socket_addr = SocketAddr::new(IpAddr::V6(parsed_ip), 19531);

        let cursor = checkpointer.lock().await.cursor.clone();

        let url = &format!("http://{}/entries", socket_addr);
        let mut last_cursor = match cursor {
            Some(val) => val,
            None => "".to_string(),
        };
        let mut batch = BATCH_SIZE;

        loop {
            let req = hyper::Request::builder()
                .method(hyper::Method::GET)
                .uri(url)
                .header(ACCEPT, "application/vnd.fdo.journal".to_string())
                .header(
                    RANGE,
                    format!(
                        "entries={}:{}:{}",
                        match last_cursor.is_empty() {
                            false => last_cursor.clone(),
                            true => "".to_string(),
                        },
                        match last_cursor.is_empty() {
                            false => 1,
                            true => -1,
                        },
                        self.batch_size
                    ),
                )
                .body(hyper::Body::empty())
                .unwrap();

            let body = tokio::select! {
                biased;
                _ = &mut shutdown => break,
                result = self.client.request(req) => match result {
                    Ok(resp) => resp.into_body(),
                    Err(_) => {
                        warn!("Couldn't reach node... Backing off");
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
                result = hyper::body::to_bytes(body) => match result {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        emit!(SystemdJournalGatewaydReadError { error });
                        continue;
                    }
                }
            };

            let log_entries = String::from_utf8_lossy(&body_bytes)
                .split("\n\n")
                .into_iter()
                .filter(|f| !f.is_empty())
                .map(|f| f.to_string())
                .collect::<Vec<String>>();

            for entry in log_entries {
                let mut log = LogEvent::default();

                entry
                    .split('\n')
                    .into_iter()
                    .for_each(|line| match line.split_once('=') {
                        None => (),
                        Some((name, value)) => {
                            log.insert(name, value);
                            ()
                        }
                    });

                let current_cursor = match log.get(CURSOR) {
                    None => {
                        warn!(
                            "Log line without cursor... Skipping..., line was: {}",
                            entry
                        );
                        continue;
                    }
                    Some(cursor) => cursor.to_string().replace('\"', ""),
                };

                // Escaping duplicated logs
                if current_cursor == last_cursor {
                    break;
                }
                last_cursor = current_cursor;

                self.out
                    .send_event(log)
                    .await
                    .map_err(|error| emit!(StreamClosedError { error, count: 1 }))?;
            }
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

#[derive(Debug)]
pub struct SystemdJournalGatewaydParseIpError {
    pub error: AddrParseError,
}

impl InternalEvent for SystemdJournalGatewaydParseIpError {
    fn emit(self) {
        error!(message = "Unable to parse systemd-journal-gatewayd endpoint",
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
pub struct SystemdJournalGatewaydReadError {
    pub error: hyper::Error,
}

impl InternalEvent for SystemdJournalGatewaydReadError {
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
pub struct SystemdJournalGatewaydJsonError {
    pub error: std::string::FromUtf8Error,
}

impl InternalEvent for SystemdJournalGatewaydJsonError {
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
