mod config;

use config::Config;

fn main() {
    match Config::load_from_file("config.yaml") {
        Ok(config) => {
            println!(
                "Loaded config: {} route(s), listening on {}:{}",
                config.routes.len(),
                config.server.host,
                config.server.port
            );
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
