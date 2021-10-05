cmake_minimum_required(VERSION 3.20)
project(shuffle)

set(CMAKE_CXX_STANDARD 17)

add_executable(shuffle main.cpp writer.h writer.cpp common.h snappy_mock.h)
