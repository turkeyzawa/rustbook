use clap::{Arg, App, AppSettings, SubCommand, _clap_count_exprs, arg_enum, value_t};
use reqwest::Client;
use std::io;

fn do_post_csv(api_client: &ApiClient) {
    let reader = csv::Reader::from_reader(io::stdin());
    for log in reader.into_deserialize::<api::logs::post::Request>() {
        let log = match log {
            Ok(log) => log,
            Err(e) => {
                eprintln!("[WARN] failed to parse a line, skipping: {}", e);
                continue;
            }
        };
        api_client.post_logs(&log).expect("api request failed.");
    }
}

fn do_get_json(api_client: &ApiClient) {
    let res = api_client.get_logs().expect("api request failed.");
    let json_str = serde_json::to_string(&res).unwrap();
    println!("{}", json_str);
}

struct ApiClient {
    server: String,
    client: Client,
}

impl ApiClient {
    fn post_logs(&self, req: &api::logs::post::Request) -> reqwest::Result<()> {
        self.client
            .post(&format!("http://{}/logs", &self.server))
            .json(req)
            .send()
            .map(|_| ())
    }
    fn get_logs(&self) -> reqwest::Result<api::logs::get::Response> {
        self.client
            .get(&format!("http://{}/logs", &self.server))
            .send()?
            .json()
    }
}

arg_enum! {
    #[derive(Debug)]
    enum Format {
        Csv,
        Json
    }
}

fn main() {
    let opts = App::new(env!("CARGO_PKG_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("SERVER")
                .short("s")
                .long("server")
                .value_name("URL")
                .help("server url")
                .takes_value(true)
        )
        .subcommand(SubCommand::with_name("post").about("post logs, taking input from stdin"))
        .subcommand(
            SubCommand::with_name("get").about("get logs").arg(
                Arg::with_name("FORMAT")
                    .help("log format")
                    .short("r")
                    .long("format")
                    .takes_value(true)
                    .possible_values(&Format::variants())
                    .case_insensitive(true)
            ),
        );
    let matches = opts.get_matches();
    let server = matches
        .value_of("SERVER")
        .unwrap_or("localhost:3000")
        .into();
    let client = Client::new();
    let api_client = ApiClient { server, client };
    match matches.subcommand() {
        ("get", sub_match) => {
            let format = sub_match
                .and_then(|m| m.value_of("FORMAT"))
                .map(|m| m.parse().unwrap())
                .unwrap();
            match format {
                Format::Csv => unimplemented!(),
                Format::Json => do_get_json(&api_client),
            }
        },
        ("post", _) => do_post_csv(&api_client),
        _ => unreachable!(),
    }
}
