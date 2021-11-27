# ipmq - Interprocess Message Queue
Message queue system inspired by RabbitMQ for interprocess communications on Linux machines.

## Features
* Routing of messages based on RabbitMQ model, where messages can be sent to multiple queues depending on the routing keys and bindings.
* Uses Unix domain sockets (UDS) for communication. The message data are allocated in shared memory to minimize copies between the producer and consumers.
* Exposes Python3, C & C++ wrapper libraries. See `samples` folder for how to use them.
* Written in Rust.
* Supports only Linux.

## Build instructions
* Requires cargo (https://rustup.rs/) to build.
* Run `build.sh` to build and package the wrapper libraries.
