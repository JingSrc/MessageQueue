cmake_minimum_required(VERSION 3.12)
project(mq)

set(CMAKE_CXX_STANDARD 11)

include_directories(
        ${PROJECT_SOURCE_DIR}/include)

add_executable(mq main.cpp MessageQueue.h SharedMutex.h ThreadPool.h ProducerConsumer.h)
