# ipmq - Interprocess Message Queue
Message queue system inspired by RabbitMQ for interprocess communications.

## Features
* Supports only Linux.
* Written in Rust.
* Uses Unix Domain Sockets (UDS) and shared memory for communication.
* Exposes both Python, C & C++ wrapper libraries. See `samples` folder for how to use them.

## Build instructions
* Requires cargo (https://rustup.rs/) to build.
* Build python wrapper: `cargo build --release --features python_wrapper`. Results in `target/release`.
* Build C wrapper: `cargo build --release --features c_wrapper`. Results in `target/release`.
