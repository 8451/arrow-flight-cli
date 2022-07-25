# Apache Arrow Flight CLI

This is a command line interface for an Apache Arrow Flight.

## Examples of Using PyArrow and Arrow Flight
https://arrow.apache.org/cookbook/py/flight.html

## Arrow Flight Spark Connector
https://github.com/rymurr/flight-spark-source

## Arrow Flight gRPC API
[Introducing Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/)

## Commands
TODO: Add Commands

## Get started with Development
1. [Install Rust](https://www.rust-lang.org/tools/install)
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
``` 
2. In the flight-cli directory,
```bash
cargo update
```
3. Run the CLI by running
```bash
cargo run -- <command> <args> <flags>
```