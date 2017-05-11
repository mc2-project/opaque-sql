cmake_minimum_required(VERSION 2.8)

project(OpaqueKeyGen)

file(GLOB SOURCES "*.cpp")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(libservice_provider.so SHARED ${SOURCES})

find_library(CRYPTO_LIB crypto)
find_library(SSL_LIB ssl)
target_link_libraries(libservice_provider.so "${CRYPTO_LIB}" "${SSL_LIB}")

add_executable(main ${SOURCES})
target_link_libraries(main "${CRYPTO_LIB}" "${SSL_LIB}")
