mod auth;
mod errors;

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use arrow::csv::WriterBuilder;
use arrow::datatypes::Schema;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{Criteria, FlightDescriptor, Ticket};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use Box;

use crate::auth::{azure_authorization, IdentityProvider, OAuth2Interceptor};
use serde::{Deserialize, Serialize};
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, Endpoint};

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
        server_address: Option<String>,
    },
    ///
    SetOauthProvider {
        #[clap(value_parser)]
        provider: String,
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
    },
    GetSchema {
        #[clap(value_parser)]
        query: String,
    },
    DoGet {
        #[clap(value_parser)]
        ticket: String,
    },
    GetTicketInfo {
        #[clap(value_parser)]
        query: String,
        #[clap(value_parser, short = 'p')]
        file_path: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConnectionConfig {
    address: String,
    identity_provider: IdentityProvider,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            address: "".into(),
            identity_provider: IdentityProvider::None,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let cfg: ConnectionConfig = confy::load_path("/Users/******/Desktop/flight-cli")?;

    match &cli.command {
        Commands::SetServer { server_address } => {
            match server_address {
                Some(address) => {
                    let new_cfg = ConnectionConfig {
                        address: address.to_owned(),
                        identity_provider: cfg.identity_provider,
                    };
                    // Test server connection before changing config
                    let client = connect_to_flight_server(&new_cfg).await;
                    match client {
                        Ok(_) => {
                            confy::store_path("/Users/******/Desktop/flight-cli", &new_cfg)?;
                            println!("Changed flight server to {}", new_cfg.address);
                            Ok(())
                        }
                        Err(e) => Err(e.into()),
                    }
                }
                None => Err("No Server Address Provided".into()),
            }
        }
        Commands::SetOauthProvider { provider } => {
            let new_cfg = ConnectionConfig {
                address: cfg.address,
                identity_provider: IdentityProvider::from_str(provider)?,
            };
            confy::store_path("/Users/******/Desktop/flight-cli", &new_cfg)?;
            println!("Set OAuth Provider to {}!", provider);
            Ok(())
        }
        Commands::GetServer => {
            println!("Currently saved server is: {:?}", &cfg);
            Ok(())
        }
        Commands::TestConnection => {
            match connect_to_flight_server(&cfg).await {
                Ok(_) => println!("Successfully connected to Flight Server!"),
                Err(e) => eprintln!("Failed to connect to Flight server with error: {}", e),
            }
            Ok(())
        }
        Commands::ListFlights => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let criteria = Criteria { expression: vec![] };
            let flights = client.list_flights(criteria).await?;
            let mut flights = flights.into_inner();
            while let Some(flight) = flights.next().await {
                println!("{}", flight?);
            }
            Ok(())
        }
        Commands::GetFlightInfo { query } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = FlightDescriptor::new_path(vec![query.to_owned()]);
            println!("Flight Descriptor: {:?}", fd);
            let flight_info = client.get_flight_info(fd).await?;
            println!("Flight info: {}", flight_info.into_inner());
            Ok(())
        }
        Commands::GetSchema { query: _query } => {
            // TODO: Fix this. It's broken
            // let mut client = connect_to_flight_server(&cfg).await?;
            // let fd = FlightDescriptor::new_path(vec!(query.to_owned()));
            // let flight_info = client.get_flight_info(fd).await?.into_inner();
            // let schema = flight_info.schema;
            // let schema = schema_from_bytes(&*schema);
            // println!("Schema: {:?}", schema);
            println!("Not currently implemented");
            Ok(())
        }
        Commands::DoGet { ticket } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let ticket = ticket.clone().into_bytes();
            let ticket = Ticket { ticket };
            let flight_data = client.do_get(ticket).await?;
            let mut flight_data = flight_data.into_inner();
            while let Some(flight) = flight_data.next().await {
                println!("{}", flight?);
            }
            Ok(())
        }
        Commands::GetTicketInfo { query, file_path } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = FlightDescriptor::new_path(vec![query.to_owned()]);
            println!("Flight Descriptor: {:?}", fd);

            let file = File::create(file_path)?;
            let mut csv_writer = WriterBuilder::new().build(file);

            let flight_info = client.get_flight_info(fd).await?.into_inner();
            for endpoint in flight_info.endpoint {
                let ticket = endpoint.ticket;
                if let Some(ticket) = ticket {
                    let mut stream = client.do_get(ticket).await?.into_inner();
                    let schema_data = stream.message().await?;
                    let schema = if let Some(schema) = schema_data {
                        Arc::new(Schema::try_from(&schema)?)
                    } else {
                        break;
                    };

                    let mut results = vec![];
                    let dictionaries_by_field = HashMap::new();
                    while let Some(flight_data) = stream.message().await? {
                        let record_batch = flight_data_to_arrow_batch(
                            &flight_data,
                            schema.clone(),
                            &dictionaries_by_field,
                        )?;
                        results.push(record_batch);
                    }

                    for batch in results {
                        csv_writer.write(&batch)?;
                    }
                }
            }
            Ok(())
        }
    }
}

// TODO: Implement this function if OAuth is set to None
// async fn connect_to_flight_server(config: &ConnectionConfig) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
//     let address = http::Uri::from_str(&config.address)?;
//     Ok(futures::executor::block_on(FlightServiceClient::connect(address))?)
// }

async fn connect_to_flight_server(
    config: &ConnectionConfig,
) -> Result<FlightServiceClient<InterceptedService<Channel, OAuth2Interceptor>>, Box<dyn Error>> {
    let token = azure_authorization().await?;
    let channel = Endpoint::from_str(&*config.address)?.connect().await?;
    Ok(FlightServiceClient::with_interceptor(
        channel,
        OAuth2Interceptor {
            access_token: token.secret().clone(),
        },
    )) // This is nasty, but should be fine as this will only be needed once.
}
