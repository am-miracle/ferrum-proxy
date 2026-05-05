mod balancing;
mod config;
mod health;
mod http;
mod routing;
mod server;
mod upstream;

use config::Config;

#[tokio::main]
async fn main() {
    match Config::load_from_file("config.yaml") {
        Ok(config) => {
            if let Err(err) = server::run(config).await {
                eprintln!("server failed: {err}");
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
