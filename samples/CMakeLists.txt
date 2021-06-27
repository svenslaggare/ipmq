cmake_minimum_required(VERSION 3.18)
project(sample)

set(CMAKE_CXX_STANDARD 14)

add_executable(sample cpp/ipmq.hpp cpp/ipmq.cpp main.cpp)
target_link_directories(sample PUBLIC "${CMAKE_SOURCE_DIR}/../target/release")
target_include_directories(sample PUBLIC "${CMAKE_SOURCE_DIR}/../target")
target_link_libraries(sample ipmq)
