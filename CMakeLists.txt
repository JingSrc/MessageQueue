cmake_minimum_required(VERSION 3.12)
project(mq)

set(CMAKE_CXX_STANDARD 11)

add_executable(mq main.cpp MessageQueue.h SharedMutex.h ThreadPool.h)
