cmake_minimum_required(VERSION 3.13)
project(shuffle)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_THREAD_PREFER_PTHREAD ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS -pthread)

add_executable(shuffle main.cpp writer.h writer.cpp common.h snappy_mock.h)