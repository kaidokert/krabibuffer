cmake_minimum_required(VERSION 3.10)
project(krabibuffer)

add_executable(shmem_reader examples/shmem_reader.cpp)
target_include_directories(shmem_reader PRIVATE include)
