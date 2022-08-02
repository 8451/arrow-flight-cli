mod auth;
mod errors;

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use arrow::csv::{Writer, WriterBuilder};
use arrow::datatypes::Schema;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::{flight_data_to_arrow_batch};
use arrow_flight::{Criteria, FlightDescriptor, Ticket};
use clap::{Parser, Subcommand};
use futures::{StreamExt};
use parquet::arrow::{ArrowWriter};
use parquet::file::properties::WriterProperties;
use Box;
use std::io;
use std::io::Write;
use oauth2::AccessToken;
use parquet::basic::Compression;

use crate::auth::{azure_authorization, IdentityProvider, OAuth2Interceptor};
use serde::{Deserialize, Serialize};
use tonic::codegen::InterceptedService;
use tonic::Status;
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
    /// Set OAuth Provider (Azure, Github, Google, Default: None)
    SetOauthProvider {
        #[clap(value_enum)]
        provider: IdentityProvider,
    },
    /// Get the current server in the config
    GetServer,
    /// Test the connection of the current flight server
    TestConnection,
    /// DEBUG: List all flights
    ListFlights,
    /// Get flight info of input query
    GetFlightInfo {
        #[clap(value_parser)]
        query: String,
    },
    /// DEBUG: Get Schema of a path
    GetSchema {
        #[clap(value_parser)]
        query: String,
    },
    /// DEBUG: Get data for a specific Flight Ticket
    DoGet {
        #[clap(value_parser)]
        ticket: String,
    },
    /// Get the data for a path
    GetData {
        #[clap(value_parser)]
        query: String,
        #[clap(value_parser, short = 'f')]
        file_path: String,
        #[clap(short = 'p', long = "parquet", action)]
        save_as_parquet: bool,
    },
    /// UNIMPLEMENTED: Send data through DoPut (requires parquet file)
    SendData {
        #[clap(value_parser, short = 'q')]
        path: String,
        #[clap(value_parser, short = 'f')]
        parquet_path: String,
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
    let cfg: ConnectionConfig = confy::load("flight-cli")?;

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
                            confy::store("flight-cli", &new_cfg)?;
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
                identity_provider: provider.to_owned()
            };
            confy::store("flight-cli", &new_cfg)?;
            println!("Set OAuth Provider to {:?}!", provider);
            Ok(())
        }
        Commands::GetServer => {
            println!("Currently saved server is: {}", &cfg.address);
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
        Commands::GetSchema { query } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = FlightDescriptor::new_path(vec!(query.to_owned()));
            let flight_info = client.get_flight_info(fd).await?.into_inner();
            let schema = Schema::try_from(flight_info.clone())?;
            println!("Schema: {:?}", schema);
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
        Commands::GetData {
            query,
            file_path,
            save_as_parquet,
        } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = FlightDescriptor::new_path(vec![query.to_owned()]);
            println!("Flight Descriptor: {:?}", fd);

            let flight_info = client.get_flight_info(fd).await?.into_inner();
            let schema = Schema::try_from(flight_info.clone())?;

            let mut csv_writer: Option<Writer<File>> = if !save_as_parquet {
                let file = File::create(file_path)?;
                Some(WriterBuilder::new().build(file))
            } else {
                None
            };
            let mut parquet_writer: Option<ArrowWriter<File>> = if *save_as_parquet { // FIX: This won't work because it will overwrite for each endpoint
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let file = File::create(file_path)?;
                Some(ArrowWriter::try_new(file, schema.into(), Some(props))?)
            } else {
                None
            };

            println!("Found {} endpoints", flight_info.endpoint.len());

            let stdout = io::stdout();
            let mut handle = stdout.lock();

            let mut endpoint_count = 0;
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
                    let mut batch_count: u64 = 0;
                    let mut results = vec![];
                    let dictionaries_by_field = HashMap::new();
                    while let Some(flight_data) = stream.message().await? {
                        let record_batch = flight_data_to_arrow_batch(
                            &flight_data,
                            schema.clone(),
                            &dictionaries_by_field,
                        )?;
                        results.push(record_batch);

                        batch_count = batch_count + 1;
                        write!(handle, "\rProcessed {} batches", batch_count)?;
                    }

                    for batch in results {
                        // This could be a place for generics? I don't like this.
                        if let Some(ref mut writer) = csv_writer {
                            writer.write(&batch)?;
                        } else if let Some(ref mut writer) = parquet_writer {
                            writer.write(&batch)?;
                        }
                    }
                    write!(handle, "\nCompleted batches for endpoint {}\n", endpoint_count)?;
                    endpoint_count = endpoint_count + 1;
                }
            }
            if let Some(writer) = parquet_writer {
                writer.close()?;
            }
            Ok(())
        }
        Commands::SendData {
            path: _,
            parquet_path: _,
        } => {
            Err(Box::try_from(Status::unimplemented("Send Data not yet implemented"))?)
        }
    }
}

async fn connect_to_flight_server(
    config: &ConnectionConfig,
) -> Result<FlightServiceClient<InterceptedService<Channel, OAuth2Interceptor>>, Box<dyn Error>> {
    let token = match config.identity_provider {
        IdentityProvider::Azure => azure_authorization().await?,
        IdentityProvider::Google => return Err(Box::try_from(Status::unimplemented("Google Auth not yet implemented"))?),
        IdentityProvider::Github => return Err(Box::try_from(Status::unimplemented("Github Auth not yet implemented"))?),
        IdentityProvider::None => AccessToken::new("".parse()?)
    };
    let channel = Endpoint::from_str(&*config.address)?.connect().await?;
    Ok(FlightServiceClient::with_interceptor(
        channel,
        OAuth2Interceptor {
            access_token: token.secret().clone(),
        }
    ))
}