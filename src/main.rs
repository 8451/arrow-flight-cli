mod commands;

use clap::{Parser, Subcommand};

/// Simple program to greet a person
#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct Cli {
    /// Name of the person to greet
    #[clap(long = "verbose")]
    verbose: bool,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Hello {
        #[clap(value_parser)]
        name: Option<String>
    }
}


fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Hello { name } => {
            println!("Hello {:?}!9", name)
        }
    }
}
