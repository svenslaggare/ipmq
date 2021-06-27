#!/bin/bash

mkdir -p build

# Build Python wrapper
cargo build --release --features python_wrapper
mkdir -p build/python
cp target/release/libipmq.so build/python

# Build C wrapper
cargo build --release --features c_wrapper
mkdir -p build/c
cp target/release/libipmq.so build/c
cp target/ipmq.h build/c

# Build C++ wrapper
cargo build --release --features c_wrapper
mkdir -p build/cpp
cp target/release/libipmq.so build/cpp
cp target/ipmq.h build/cpp
cp samples/cpp/ipmq.cpp build/cpp
cp samples/cpp/ipmq.hpp build/cpp
