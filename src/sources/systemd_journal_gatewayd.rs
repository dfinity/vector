//! Generalized HTTP client source.
//! Calls an endpoint at an interval, decoding the HTTP responses into events.

use crate::internal_events::{
    JournaldCheckpointFileOpenError, StreamClosedError, TcpSocketOutgoingConnectionError,
};
use crate::sources::journald::{Finalizer, SharedCheckpointer, StatefulCheckpointer};
use crate::SourceSender;
use crate::{
    config::{SourceConfig, SourceContext},
    sources,
};
use metrics::counter;
use serde_json::Value as JsonValue;
use std::net::SocketAddr;
use std::net::{AddrParseError, IpAddr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpSocket, TcpStream};
use vector_common::internal_event::{error_stage, error_type, InternalEvent};
use vector_common::shutdown::ShutdownSignal;
use vector_config::configurable_component;
use vector_core::config::{DataType, LogNamespace, Output, SourceAcknowledgementsConfig};
use vector_core::event::LogEvent;
use vector_core::serde::bool_or_struct;

const CHECKPOINT_FILENAME: &str = "checkpoint.txt";
const _CURSOR: &str = "__CURSOR";

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

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,
}

impl_generate_config_from_default!(SystemdJournalGatewaydConfig);

#[async_trait::async_trait]
impl SourceConfig for SystemdJournalGatewaydConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let data_dir = cx
            .globals
            .resolve_and_make_data_subdir(self.data_dir.as_ref(), cx.key.id())?;

        let mut checkpoint_path = data_dir;
        checkpoint_path.push(CHECKPOINT_FILENAME);

        let acknowledgements = cx.do_acknowledgements(self.acknowledgements);

        Ok(Box::pin(
            SystemdJournalGatewaydSource {
                endpoint: self.endpoint.clone(),
                out: cx.out,
                checkpoint_path,
                acknowledgements,
            }
            .run_shutdown(cx.shutdown),
        ))
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
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
    acknowledgements: bool,
}

impl SystemdJournalGatewaydSource {
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
        let finalizer = Finalizer::new(
            self.acknowledgements,
            checkpointer.clone(),
            shutdown.clone(),
        );

        self.run(shutdown, checkpointer, finalizer).await?;

        Ok(())
    }

    async fn run(
        mut self,
        mut shutdown: ShutdownSignal,
        checkpointer: SharedCheckpointer,
        _finalizer: Finalizer,
    ) -> Result<(), ()> {
        let parsed_ip = self.endpoint.parse().map_err(|error| {
            emit!(SystemdJournalGatewaydParseIpError { error });
        })?;
        let socket_addr = SocketAddr::new(IpAddr::V6(parsed_ip), 19531);

        let socket = TcpSocket::new_v6()
            .map_err(|error| emit!(TcpSocketOutgoingConnectionError { error }))?;

        let mut stream = socket
            .connect(socket_addr)
            .await
            .map_err(|error| emit!(TcpSocketOutgoingConnectionError { error }))?;

        let cursor = checkpointer.lock().await.cursor.clone();

        if let Some(cursor) = cursor {
            stream.write_all(format!("GET /entries?follow HTTP/1.1\nAccept: application/json\nRange: entries={}:0:\n\r\n\r", cursor).as_bytes()).await.map_err(|error| {
                emit!(TcpSocketOutgoingConnectionError { error })
            })?;
        } else {
            stream.write_all(b"GET /entries?follow HTTP/1.1\nAccept: application/json\nRange: entries:-1:\n\r\n\r").await.map_err(|error| {
                emit!(TcpSocketOutgoingConnectionError { error })
            })?;
        }

        loop {
            let last_cursor = self.run_stream(&mut stream).await?;

            //Backoff
            let timeout = tokio::time::sleep(Duration::from_millis(10));
            tokio::pin!(timeout);
            let mut should_stop = false;
            tokio::select! {
                _ = &mut shutdown => {
                    should_stop = true;
                },
                _ = &mut timeout => {},
            }

            checkpointer.lock().await.set(last_cursor).await;
            if should_stop {
                break;
            }
        }

        Ok(())
    }

    async fn run_stream(&mut self, stream: &mut TcpStream) -> Result<String, ()> {
        let bf = BufReader::new(stream);
        let mut lines = bf.lines();
        let mut last_cursor = String::new();
        let mut current_batch_size = 16;
        while current_batch_size != 0 {
            if let Some(line) = lines
                .next_line()
                .await
                .map_err(|error| emit!(SystemdJournalGatewaydReadError { error }))?
            {
                if let Some('{') = line.chars().next() {
                    current_batch_size = current_batch_size - 1;
                    let record = match serde_json::from_str::<JsonValue>(line.as_str()) {
                        Ok(record) => record,
                        Err(_) => {
                            info!("Had to fix the logs...");
                            let line = line.replace("\\", "").replace("}}}\"", "}}");
                            serde_json::from_str::<JsonValue>(line.as_str())
                                .map_err(|error| emit!(SystemdJournalGatewaydJsonError { error }))?
                        }
                    };

                    last_cursor = record.get("__CURSOR").unwrap().to_string();

                    let mut log = LogEvent::default();
                    log.insert("message", line);
                    self.out
                        .send_event(log)
                        .await
                        .map_err(|error| emit!(StreamClosedError { error, count: 1 }))?;
                }
            }
        }

        Ok(last_cursor)
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
    pub error: std::io::Error,
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
    pub error: serde_json::Error,
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
