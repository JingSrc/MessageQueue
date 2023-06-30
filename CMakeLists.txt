cmake_minimum_required(VERSION 3.12)
project(mq)

set(CMAKE_CXX_STANDARD 11)

add_executable(mq main.cpp MessageQueue.h)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
