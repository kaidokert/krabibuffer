cmake_minimum_required(VERSION 3.10)
project(krabibuffer)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(shmem_reader examples/shmem_reader.cpp)
target_include_directories(shmem_reader PRIVATE include)
