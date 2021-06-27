# ipmq - Interprocess Message Queue
Message queue system inspired by RabbitMQ for interprocess communications on Linux machines.

## Features
* RabbitMQ inspired routing of messages when messages can be sent to multiple queues due to routing keys and bindings.
* Uses Unix domain sockets (UDS) for communication. The message data are allocated in shared memory to minimizes copies between producer and consumer.
* Exposes Python (3), C & C++ wrapper libraries. See `samples` folder for how to use them.
* Written in Rust.
* Supports only Linux.

## Build instructions
* Requires cargo (https://rustup.rs/) to build.
* Run `build.sh` to build and package the wrapper libraries.