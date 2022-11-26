//! Generalized HTTP client source.
//! Calls an endpoint at an interval, decoding the HTTP responses into events.

use crate::internal_events::{
    StreamClosedError, SystemdJournalGatewaydParseIpError, SystemdJournalGatewaydReadError,
    TcpSocketOutgoingConnectionError,
};
use crate::SourceSender;
use crate::{
    config::{SourceConfig, SourceContext},
    sources,
};
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpSocket;
use vector_common::shutdown::ShutdownSignal;
use vector_config::configurable_component;
use vector_core::config::{DataType, LogNamespace, Output};
use vector_core::event::LogEvent;

/// Configuration for the `systemd_journal_gatewayd` source.
#[configurable_component(source("systemd_journal_gatewayd"))]
#[derive(Clone, Debug, Default)]
pub struct SystemdJournalGatewaydConfig {
    /// Endpoint to collect events from. The full path must be specified.
    /// Example: "::1"
    pub endpoint: String,
}

impl_generate_config_from_default!(SystemdJournalGatewaydConfig);

#[async_trait::async_trait]
impl SourceConfig for SystemdJournalGatewaydConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        Ok(Box::pin(
            SystemdJournalGatewaydSource {
                endpoint: self.endpoint.clone(),
                out: cx.out,
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
}

impl SystemdJournalGatewaydSource {
    async fn run_shutdown(self, mut shutdown: ShutdownSignal) -> Result<(), ()> {
        let should_run = Arc::new(AtomicBool::new(true));

        let join_handle = tokio::task::spawn(self.run(should_run.clone()));

        tokio::select! {
            _ = &mut shutdown => should_run.store(false, Ordering::Relaxed),
        }

        if let Err(_) = join_handle.await {
            warn!("Systemd-journal-gatewayd task returned an error");
        }

        Ok(())
    }

    async fn run(mut self, should_run: Arc<AtomicBool>) -> Result<(), ()> {
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

        stream.write_all(b"GET /entries?follow HTTP/1.1\nAccept: application/json\nRange: entries:-1:\n\r\n\r").await.map_err(|error| {
            emit!(TcpSocketOutgoingConnectionError { error })
        })?;

        let bf = BufReader::new(stream);
        let mut lines = bf.lines();

        while should_run.load(Ordering::Relaxed) {
            if let Some(line) = lines
                .next_line()
                .await
                .map_err(|error| emit!(SystemdJournalGatewaydReadError { error }))?
            {
                if let Some('{') = line.chars().next() {
                    let mut log = LogEvent::default();
                    log.insert("message", line);
                    self.out
                        .send_event(log)
                        .await
                        .map_err(|error| emit!(StreamClosedError { error, count: 1 }))?;
                }
            }
        }

        Ok(())
    }
}
