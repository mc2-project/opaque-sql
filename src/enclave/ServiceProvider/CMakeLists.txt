cmake_minimum_required(VERSION 2.8)

project(OpaqueKeyGen)

file(GLOB SOURCES "*.cpp")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")

add_library(libservice_provider.so SHARED ${SOURCES})

target_link_libraries(libservice_provider.so crypto ssl)

add_executable(main ${SOURCES})
