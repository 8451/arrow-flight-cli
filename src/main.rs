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
use std::io::Write;
use std::{fs, io};

use crate::auth::{azure_authorization, IdentityProvider, OAuth2Interceptor};
use serde::{Deserialize, Serialize};
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
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

    /// Provide path to root CA instead of using config value (Must be used with args client-certificate & private-key)
    #[clap(long = "ca-certificate")]
    ca_certificate_path: Option<String>,

    /// Provide path to client certificate instead of using config value (Must be used with args ca-certificate & private-key)
    #[clap(long = "client-certificate")]
    client_certificate_path: Option<String>,

    /// Provide path to private key instead of using config value (Must be used with args ca-certificate & client-certificate)
    #[clap(long = "private-key")]
    private_key_path: Option<String>,

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
    /// Set your own root CA and/or use self-signed certificates
    SetTlsCerts {
        #[clap(value_parser)]
        ca_cert_path: String,
        #[clap(value_parser, short = 'c', long = "client-cert")]
        client_cert_path: Option<String>,
        #[clap(value_parser, short = 'k', long = "private-key")]
        private_key_path: Option<String>,
    },
    ClearTlsCerts {
        #[clap(long = "disable-tls", short = 'd')]
        disable_tls: bool,
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
    tls_certs: Option<TlsCertPaths>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            address: "".into(),
            identity_provider: IdentityProvider::None,
            scope: "".into(),
            tls: false,
            tls_certs: None,
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
                tls_certs: None,
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
                tls_certs: None,
            };
            confy::store("flight-cli", &new_cfg)?;
            println!(
                "Set OAuth Provider to {:?} with scope \"{}\"",
                provider, scope
            );
            Ok(())
        }
        Commands::SetTlsCerts {
            ca_cert_path,
            client_cert_path,
            private_key_path,
        } => {
            let tls_paths = TlsCertPaths {
                ca_crt: ca_cert_path.to_owned(),
                client_crt: client_cert_path.to_owned(),
                private_key: private_key_path.to_owned(),
            };
            let new_cfg = ConnectionConfig {
                address: cfg.address,
                identity_provider: cfg.identity_provider,
                scope: cfg.scope,
                tls: true,
                tls_certs: Some(tls_paths),
            };
            confy::store("flight-cli", &new_cfg)?;
            Ok(())
        }
        Commands::ClearTlsCerts { disable_tls } => {
            let new_cfg = ConnectionConfig {
                address: cfg.address,
                identity_provider: cfg.identity_provider,
                scope: cfg.scope,
                tls: !*disable_tls,
                tls_certs: None,
            };
            confy::store("flight-cli", &new_cfg)?;
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
        if let Some(tls_paths) = &config.tls_certs {
            let ca_cert = Certificate::from_pem(fs::read_to_string(&tls_paths.ca_crt)?);
            // A user can use root CA without having client certs, but a private key should exist if client_cert exists
            let client_identity = if let (Some(client_cert), Some(private_key)) =
                (&tls_paths.client_crt, &tls_paths.private_key)
            {
                let client_cert = fs::read_to_string(client_cert)?;
                let private_key = fs::read_to_string(&private_key)?;
                Some(Identity::from_pem(client_cert, private_key))
            } else {
                None
            };
            let tls_config = if let Some(client_identity) = client_identity {
                ClientTlsConfig::new()
                    .ca_certificate(ca_cert)
                    .identity(client_identity)
            } else {
                ClientTlsConfig::new().ca_certificate(ca_cert)
            };
            Endpoint::from_str(&*config.address)?
                .tls_config(tls_config)?
                .connect()
                .await?
        } else {
            // Use root CA from OS
            Endpoint::from_str(&*config.address)?
                .tls_config(ClientTlsConfig::new())?
                .connect()
                .await?
        }
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
    let tls_certs = {
        let mut result = if let Some(config) = &config.tls_certs {
            TlsCertPaths {
                ca_crt: config.ca_crt.to_owned(),
                client_crt: config.client_crt.to_owned(),
                private_key: config.private_key.to_owned(),
            }
        } else {
            TlsCertPaths {
                ca_crt: "".to_string(),
                client_crt: None,
                private_key: None,
            }
        };
        if let Some(ca_crt) = &cli.ca_certificate_path {
            result.ca_crt = ca_crt.to_owned();
        }
        if let Some(client_crt) = &cli.client_certificate_path {
            result.client_crt = Some(client_crt.to_owned());
        }
        if let Some(private_key) = &cli.private_key_path {
            result.private_key = Some(private_key.to_owned());
        }
        Some(result)
    };
    // TLS will always be true if CLI option set, but could be true or false if not set
    let tls = if cli.tls { true } else { config.tls };
    ConnectionConfig {
        address,
        identity_provider,
        scope,
        tls,
        tls_certs,
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

/// A struct holding the paths to different TLS certs, for if a user doesn't want to use default certs
#[derive(Serialize, Deserialize, Debug)]
struct TlsCertPaths {
    ca_crt: String,
    client_crt: Option<String>,
    private_key: Option<String>,
}
