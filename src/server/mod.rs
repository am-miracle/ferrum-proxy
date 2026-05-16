use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::signal;
use tokio::sync::{Notify, watch};
use tokio::time;

use crate::config::Config;
use crate::http::{AppState, ConnectionInfo, handle_request};

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    run_with_shutdown_kind(config, wait_for_shutdown_signal()).await
}

pub async fn run_with_shutdown<F>(
    config: Config,
    shutdown_signal: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()> + Send,
{
    run_with_shutdown_kind(config, async move {
        shutdown_signal.await;
        ShutdownReason::Terminate("test")
    })
    .await
}

async fn run_with_shutdown_kind<F>(
    config: Config,
    shutdown_signal: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ShutdownReason> + Send,
{
    let addr = config.server.socket_addr()?;
    let route_count = config.routes.len();
    let state = AppState::new(config);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    state.spawn_background_tasks(shutdown_rx.clone());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let tracker = Arc::new(ConnectionTracker::default());
    state.run_startup_health_check().await?;

    state
        .telemetry()
        .log_server_start(&addr.to_string(), route_count);

    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, remote_addr) = result?;
                let io = TokioIo::new(stream);
                let state = state.clone();
                let telemetry = state.telemetry_handle();
                let tracker = tracker.clone();
                let mut shutdown_rx = shutdown_rx.clone();
                let client_header_timeout = state.client_header_timeout();
                tracker.open();

                tokio::spawn(async move {
                    let service = service_fn(move |mut request| {
                        let state = state.clone();
                        request.extensions_mut().insert(ConnectionInfo { remote_addr });
                        async move { Ok::<_, std::convert::Infallible>(handle_request(request, state).await) }
                    });

                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new());
                    builder.header_read_timeout(client_header_timeout);
                    let connection = builder.serve_connection(io, service);
                    tokio::pin!(connection);

                    loop {
                        tokio::select! {
                            result = &mut connection => {
                                if let Err(err) = result {
                                    telemetry.log_connection_error(&remote_addr.to_string(), &err.to_string());
                                }
                                tracker.close();
                                break;
                            }
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    connection.as_mut().graceful_shutdown();
                                }
                            }
                        }
                    }
                });
            }
            reason = &mut shutdown_signal => {
                let drain_timeout = state.shutdown_timeout();
                state.telemetry().log_control_signal(reason.signal_name(), reason.action_name());
                state.telemetry().log_shutdown_started(drain_timeout);
                let _ = shutdown_tx.send(true);
                let drained = tracker.wait_for_zero(drain_timeout).await;
                state.telemetry().log_shutdown_complete(drained, tracker.active());
                break;
            }
        }
    }

    Ok(())
}

#[derive(Default)]
struct ConnectionTracker {
    active: AtomicUsize,
    drained: Notify,
}

enum ShutdownReason {
    Terminate(&'static str),
    // SIGHUP triggers a graceful drain-and-exit; use your process supervisor to re-exec.
    Reload(&'static str),
}

impl ShutdownReason {
    fn signal_name(&self) -> &'static str {
        match self {
            Self::Terminate(signal) | Self::Reload(signal) => signal,
        }
    }

    fn action_name(&self) -> &'static str {
        match self {
            Self::Terminate(_) => "shutdown",
            Self::Reload(_) => "graceful_shutdown",
        }
    }
}

async fn wait_for_shutdown_signal() -> ShutdownReason {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut hangup = signal(SignalKind::hangup()).expect("failed to install SIGHUP handler");
        tokio::select! {
            _ = signal::ctrl_c() => ShutdownReason::Terminate("ctrl_c"),
            _ = hangup.recv() => ShutdownReason::Reload("sighup"),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
        ShutdownReason::Terminate("ctrl_c")
    }
}

impl ConnectionTracker {
    fn open(&self) {
        self.active.fetch_add(1, Ordering::SeqCst);
    }

    fn close(&self) {
        if self.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.drained.notify_waiters();
        }
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    async fn wait_for_zero(&self, timeout: std::time::Duration) -> bool {
        if self.active() == 0 {
            return true;
        }

        time::timeout(timeout, async {
            while self.active() != 0 {
                self.drained.notified().await;
            }
        })
        .await
        .is_ok()
    }
}
