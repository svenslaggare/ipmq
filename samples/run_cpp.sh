#!/bin/bash
set -e

mkdir -p build
pushd build
cmake ..
make all
popd
./build/sample $@

