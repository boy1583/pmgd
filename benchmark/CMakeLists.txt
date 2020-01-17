cmake_minimum_required(VERSION 2.8)

PROJECT(pmgd_benchmark)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../bin)

include_directories(${CMAKE_SOURCE_DIR}/../include ${CMAKE_SOURCE_DIR}/../third_party ${CMAKE_SOURCE_DIR}/../util)

link_directories(${CMAKE_SOURCE_DIR}/../lib)

link_libraries(pmgd)

add_executable(node_edge_benchmark node_edge_benchmark.cpp)