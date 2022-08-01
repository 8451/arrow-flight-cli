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
use futures::StreamExt;
use parquet::arrow::{ArrowWriter};
use parquet::file::properties::WriterProperties;
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
    /// UNIMPLEMENTED: Get Schema of a table
    GetSchema {
        #[clap(value_parser)]
        query: String,
    },
    /// Get data for a specific Flight Ticket
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
                let props = WriterProperties::builder().build();
                let file = File::create(file_path)?;
                Some(ArrowWriter::try_new(file, schema.into(), Some(props))?)
            } else {
                None
            };

            println!("Found {} endpoints", flight_info.endpoint.len());

            let stdout = io::stdout();
            let mut handle = io::BufWriter::new(stdout.lock());

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
                        handle.flush()?; // After testing, it was found that the performance impact of flushing every time was negligible/within margin of error
                    }

                    for batch in results {
                        // This could be a place for generics? I don't like this.
                        if let Some(ref mut writer) = csv_writer {
                            writer.write(&batch)?;
                        } else if let Some(ref mut writer) = parquet_writer {
                            writer.write(&batch)?;
                        }
                    }
                    write!(handle, "\nCompleted batches for endpoint {}", endpoint_count)?;
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
            // let mut client = connect_to_flight_server(&cfg).await?;
            // let file = tokio::fs::File::open(parquet_path).await?;
            // let builder = ParquetRecordBatchStreamBuilder::new(file)
            //     .await?;
            //
            // let file_metadata = builder.metadata().file_metadata();
            // let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1,2,6]);
            //
            // let stream = builder.with_projection(mask).build()?;
            //
            // stream.
            //
            // for record_batch_result in record_batch_reader {
            //     let record_batch = record_batch_result?;
            //     let ipc_write_options = IpcWriteOptions::try_new(0, false, MetadataVersion::V5)?;
            //     let flight_data = flight_data_from_arrow_batch(&record_batch, &ipc_write_options);
            //     let mut put_result = client.do_put(flight_data).await?.into_inner();
            //     while let Some(result) = put_result.next().await {
            //         println!("Put Result: {:?}", result?)
            //     }
            // }
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
