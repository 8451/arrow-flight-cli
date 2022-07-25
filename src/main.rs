mod commands;
mod errors;
mod azure_auth;

use std::io;
use std::str::FromStr;
use arrow_flight::{Criteria, FlightDescriptor};
use clap::{Parser, Subcommand};
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::{StreamExt};
use serde::{Serialize, Deserialize};
use crate::errors::CliError;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct Cli {
    #[clap(long = "verbose")]
    verbose: bool, // Not currently implemented

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a Flight Server to your config.
    SetServer {
        #[clap(value_parser)]
        server_address: Option<String>
    },
    SetAuth {
        #[clap(value_parser, long, short)]
        tenant_id: Option<String>,
        #[clap(value_parser, long, short)]
        client_id: Option<String>,
        #[clap(value_parser, long, short)]
        client_secret: Option<String>
    },
    /// Get the current server in the config
    GetServer,
    /// Test the connection of the current flight server
    TestConnection,
    /// List all flights
    ListFlights,
    /// Get flight info of input query
    GetFlightInfo {
        #[clap(value_parser)]
        query: String,
        #[clap(required = false)]
        path: bool,
    },
    GetSchema {
        #[clap(value_parser)]
        query: String,
        #[clap(required = false)]
        path: bool,
    }
}

#[derive(Serialize, Deserialize)]
struct ConnectionConfig {
    address: String,
    // tenant_id: String,
    // client_id: String,
    // client_secret: String,
    // token: String
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            address: "".into(),
            // tenant_id: "".into(),
            // client_id: "".into(),
            // client_secret: "".into(),
            // token: "".into()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let cli = Cli::parse();
    let cfg: ConnectionConfig = confy::load("flight-cli")?;

    match &cli.command {
        Commands::SetServer { server_address } => {
            match server_address {
                Some(address) => {
                    let new_cfg = ConnectionConfig {
                        address: address.to_owned(),
                        // tenant_id: cfg.tenant_id,
                        // client_id: cfg.client_id,
                        // client_secret: cfg.client_secret,
                        // token: cfg.token
                    };
                    // Test server connection before changing config
                    let client = connect_to_flight_server(&new_cfg);
                    match client {
                        Ok(_) => {
                            confy::store("flight-cli", &new_cfg)?;
                            println!("Changed flight server to {}", new_cfg.address);
                            Ok(())
                        },
                        Err(e) => {
                            Err(e)
                        }
                    }
                },
                None => Err(CliError::NoServerAddressError)
            }
        },
        Commands::SetAuth { tenant_id, client_id, client_secret } => {
        //     let mut new_cfg = ConnectionConfig {
        //         address: cfg.address,
        //         tenant_id: tenant_id.unwrap(),
        //         client_id: client_id.unwrap(),
        //         client_secret: client_secret.unwrap(),
        //         token: cfg.token
        //     };
        //
            Ok(())
        },
        Commands::GetServer => {
            println!("Currently saved server is: {}", &cfg.address);
            Ok(())
        },
        Commands::TestConnection => {
            match connect_to_flight_server(&cfg) {
                Ok(_) => println!("Successfully connected to Flight Server!"),
                Err(e) => eprintln!("Failed to connect to Flight server with error: {}", e)
            }
            Ok(())
        },
        Commands::ListFlights => {
            let mut client = connect_to_flight_server(&cfg)?;
            let criteria = Criteria {
                expression: vec![]
            };
            let flights = client.list_flights(criteria).await?;
            let mut flights = flights.into_inner();
            while let Some(flight) = flights.next().await {
                println!("{}", flight?);
            }
            Ok(())
        },
        Commands::GetFlightInfo { path: _path, query } => {
            let mut client = connect_to_flight_server(&cfg)?;
            let fd = FlightDescriptor::new_path(vec!(query.to_owned()));
            println!("Flight Descriptor: {:?}", fd);
            let flight_info = client.get_flight_info(fd).await?;
            println!("Flight info: {}", flight_info.into_inner());
            Ok(())
        },
        Commands::GetSchema { path: _path, query } => {
            let mut client = connect_to_flight_server(&cfg)?;
            let fd = FlightDescriptor::new_path(vec!(query.to_owned()));
            let schema = client.get_schema(fd).await?;
            println!("Schema: {:?}", schema.into_inner());
            Ok(())
        }
    }
}

fn connect_to_flight_server(config: &ConnectionConfig) -> Result<FlightServiceClient<tonic::transport::Channel>, CliError> {
    let address = http::Uri::from_str(&config.address)?;
    Ok(futures::executor::block_on(FlightServiceClient::connect(address))?)
}
