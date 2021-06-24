# ipmq - Interprocess Message Queue
Message queue system inspired by RabbitMQ for interprocess communications.

## Features
* Supports only Linux.
* Written in Rust.
* Uses Unix Domain Sockets (UDS) and shared memory for communication.
* Exposes both Python and C wrapper libraries. See `samples` folder for how to use them.

## Build
* Build python wrapper: `build --release --features python_wrapper`. Results in `target/release`.
* Build C wrapper: `build --release --features cn_wrapper`. Results in `target/release`.
