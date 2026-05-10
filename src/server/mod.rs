use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::signal;

use crate::config::Config;
use crate::http::{handle_request, AppState};

pub async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let addr = config.server.socket_addr()?;
    let route_count = config.routes.len();
    let state = AppState::new(config);
    state.spawn_background_tasks();
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Listening on http://{addr} with {route_count} configured route(s)");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let io = TokioIo::new(stream);
                let state = state.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let state = state.clone();
                        async move { Ok::<_, std::convert::Infallible>(handle_request(request, state).await) }
                    });

                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        eprintln!("connection error: {err}");
                    }
                });
            }
            _ = signal::ctrl_c() => {
                println!("shutdown signal received, stopping");
                break;
            }
        }
    }

    Ok(())
}
