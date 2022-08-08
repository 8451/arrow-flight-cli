mod auth;
mod errors;

use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use arrow::csv::{Writer, WriterBuilder};
use arrow::datatypes::Schema;

use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{Criteria, FlightDescriptor, FlightInfo, Ticket};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use oauth2::AccessToken;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io;
use std::io::Write;

use crate::auth::{azure_authorization, IdentityProvider, OAuth2Interceptor};
use serde::{Deserialize, Serialize};
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Status;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct Cli {
    #[clap(long = "verbose", short = 'v')]
    verbose: bool, // Not currently implemented

    // The following are options that override the values in the config
    /// Use TLS for request (If used with self-server, sets TLS use to default)
    #[clap(long = "tls", short = 't')]
    tls: bool,

    /// Provide server instead of using config value
    #[clap(long = "server", short = 's')]
    address: Option<String>,

    /// Provide OAuth Provider instead of using config value
    #[clap(long = "provider", short = 'i', value_enum)]
    identity_provider: Option<IdentityProvider>,

    /// Provide OAuth2 Scope instead of using config value
    #[clap(long = "scope", short = 'c')]
    scope: Option<String>,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a Flight Server to your config.
    SetServer {
        #[clap(value_parser)]
        server_address: String,
        #[clap(long = "tls", short = 't')]
        tls: bool,
    },
    /// Set OAuth Provider (Azure, Github, Google, Default: None)
    SetOauthProvider {
        #[clap(value_enum)]
        provider: IdentityProvider,
        #[clap(value_parser)]
        scope: String,
    },
    /// Get the current server in the config
    CurrentServer,
    /// Test the connection of the current flight server
    TestConnection,
    /// DEBUG: List all flights
    ListFlights,
    /// Get flight info of input query
    GetFlightInfo {
        #[clap(value_parser)]
        query: String,
        #[clap(
            short = 'q',
            long = "query",
            help = "Use an SQL Query instead of a table name"
        )]
        is_cmd: bool,
    },
    /// DEBUG: Get Schema of a path
    GetSchema {
        #[clap(value_parser)]
        query: String,
        #[clap(
            short = 'q',
            long = "query",
            help = "Use an SQL Query instead of a table name"
        )]
        is_cmd: bool,
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
        #[clap(
            value_enum,
            short = 'o',
            long = "format",
            help = "Output file format",
            default_value = "csv"
        )]
        file_format: DataFileFormat,
        #[clap(
            short = 'q',
            long = "query",
            help = "Use an SQL Query instead of a table name"
        )]
        is_cmd: bool,
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
    scope: String,
    tls: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            address: "".into(),
            identity_provider: IdentityProvider::None,
            scope: "".into(),
            tls: false,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let cfg: ConnectionConfig = confy::load("flight-cli")?;
    let tls_flag: &bool = &cli.tls;

    let cfg = merge_cfg_and_options(cfg, &cli);

    match &cli.command {
        Commands::SetServer {
            server_address,
            tls,
        } => {
            let tls = *tls || *tls_flag; // User can use global option or subcommand flag to set to TLS
            let new_cfg = ConnectionConfig {
                address: server_address.to_owned(),
                identity_provider: cfg.identity_provider,
                scope: cfg.scope,
                tls,
            };
            // Test server connection before changing config
            let client = connect_to_flight_server(&new_cfg).await;
            match client {
                Ok(_) => {
                    confy::store("flight-cli", &new_cfg)?;
                    println!("Changed flight server to {}", new_cfg.address);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        Commands::SetOauthProvider { provider, scope } => {
            let new_cfg = ConnectionConfig {
                address: cfg.address,
                identity_provider: provider.to_owned(),
                scope: scope.to_owned(),
                tls: cfg.tls,
            };
            confy::store("flight-cli", &new_cfg)?;
            println!(
                "Set OAuth Provider to {:?} with scope \"{}\"",
                provider, scope
            );
            Ok(())
        }
        Commands::CurrentServer => {
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
        Commands::GetFlightInfo { query, is_cmd } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = create_flight_descriptor(query.clone(), is_cmd);
            println!("Flight Descriptor: {:?}", fd);
            let flight_info = client.get_flight_info(fd).await?;
            println!("Flight info: {}", flight_info.into_inner());
            Ok(())
        }
        Commands::GetSchema { query, is_cmd } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = create_flight_descriptor(query.clone(), is_cmd);
            let flight_info = client.get_flight_info(fd).await?.into_inner();
            let schema = Schema::try_from(flight_info)?;
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
            file_format,
            is_cmd,
        } => {
            let mut client = connect_to_flight_server(&cfg).await?;
            let fd = create_flight_descriptor(query.clone(), is_cmd);
            println!("Flight Descriptor: {:?}", fd);

            let flight_info = client.get_flight_info(fd).await?.into_inner();
            let schema = Schema::try_from(flight_info.clone())?;

            let writer: Box<dyn FileWriter> = match file_format {
                DataFileFormat::Csv => {
                    let file = File::create(file_path)?;
                    Box::new(WriterBuilder::new().build(file))
                }
                DataFileFormat::Parquet => {
                    let props = WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .build();
                    let file = File::create(file_path)?;
                    Box::new(ArrowWriter::try_new(file, schema.into(), Some(props))?)
                }
            };

            println!("Found {} endpoints", flight_info.endpoint.len());

            get_data(flight_info, writer, client).await?;

            Ok(())
        }
        Commands::SendData {
            path: _,
            parquet_path: _,
        } => Err(Box::try_from(Status::unimplemented(
            "Send Data not yet implemented",
        ))?),
    }
}

async fn get_data(
    flight_info: FlightInfo,
    mut writer: Box<dyn FileWriter>,
    mut client: FlightServiceClient<InterceptedService<Channel, OAuth2Interceptor>>,
) -> Result<(), Box<dyn Error>> {
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
            let dictionaries_by_field = HashMap::new();
            while let Some(flight_data) = stream.message().await? {
                let record_batch = flight_data_to_arrow_batch(
                    &flight_data,
                    schema.clone(),
                    &dictionaries_by_field,
                )?;

                batch_count += 1;
                write!(handle, "\rProcessed {} batches", batch_count)?;

                writer.write_file(&record_batch)?;
            }

            write!(
                handle,
                "\nCompleted batches for endpoint {}\n",
                endpoint_count
            )?;
            endpoint_count += 1;
        }
    }
    writer.close_writer()?;
    Ok(())
}

async fn connect_to_flight_server(
    config: &ConnectionConfig,
) -> Result<FlightServiceClient<InterceptedService<Channel, OAuth2Interceptor>>, Box<dyn Error>> {
    let token = match config.identity_provider {
        IdentityProvider::Azure => azure_authorization(config.scope.to_owned()).await?,
        IdentityProvider::Google => {
            return Err(Box::try_from(Status::unimplemented(
                "Google Auth not yet implemented",
            ))?)
        }
        IdentityProvider::Github => {
            return Err(Box::try_from(Status::unimplemented(
                "Github Auth not yet implemented",
            ))?)
        }
        IdentityProvider::None => AccessToken::new("".parse()?),
    };
    let channel = if config.tls {
        Endpoint::from_str(&*config.address)?
            .tls_config(ClientTlsConfig::new())?
            .connect()
            .await?
    } else {
        Endpoint::from_str(&*config.address)?.connect().await?
    };
    Ok(FlightServiceClient::with_interceptor(
        channel,
        OAuth2Interceptor {
            access_token: token.secret().clone(),
        },
    ))
}

fn create_flight_descriptor(query: String, is_cmd: &bool) -> FlightDescriptor {
    if *is_cmd {
        FlightDescriptor::new_cmd(query.into_bytes())
    } else {
        FlightDescriptor::new_path(vec![query])
    }
}

/// Check if CLI Values override any of the cfg values. (Consumes config)
fn merge_cfg_and_options(config: ConnectionConfig, cli: &Cli) -> ConnectionConfig {
    let address = match &cli.address {
        Some(address) => address.to_owned(),
        None => config.address.to_owned(),
    };
    let identity_provider = match &cli.identity_provider {
        Some(provider) => provider.to_owned(),
        None => config.identity_provider.to_owned(),
    };
    let scope = match &cli.scope {
        Some(scope) => scope.to_owned(),
        None => config.scope.to_owned(),
    };
    // TLS will always be true if CLI option set, but could be true or false if not set
    let tls = if cli.tls { true } else { config.tls };
    ConnectionConfig {
        address,
        identity_provider,
        scope,
        tls,
    }
}

trait FileWriter {
    fn write_file(&mut self, record_batch: &RecordBatch) -> Result<(), Box<dyn Error>>;
    fn close_writer(self: Box<Self>) -> Result<(), Box<dyn Error>>;
}

impl FileWriter for ArrowWriter<File> {
    fn write_file(&mut self, record_batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
        self.write(record_batch)?;
        Ok(())
    }
    fn close_writer(self: Box<Self>) -> Result<(), Box<dyn Error>> {
        self.close()?;
        Ok(())
    }
}

impl FileWriter for Writer<File> {
    fn write_file(&mut self, record_batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
        self.write(record_batch)?;
        Ok(())
    }
    fn close_writer(self: Box<Self>) -> Result<(), Box<dyn Error>> {
        Ok(())
    } // CSV Writer does not implement close
}

#[derive(Serialize, Deserialize, Debug, clap::ValueEnum, Clone)]
enum DataFileFormat {
    Csv,
    Parquet,
}
