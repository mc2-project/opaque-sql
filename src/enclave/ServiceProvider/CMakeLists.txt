cmake_minimum_required(VERSION 3.13)

project(OpaqueKeyGen)

set(SOURCES
  ServiceProvider.cpp
  ServiceProviderJNI.cpp
  agent_wget.cpp
  base64.c
  crypto.cpp
  ecp.cpp
  iasrequest.cpp
  sp_crypto.cpp)

link_directories(${OE_LIBDIR})
link_directories(${OE_LIBDIR}/openenclave/enclave)
include_directories(${OE_INCLUDEDIR})
include_directories(${OE_INCLUDEDIR}/openenclave/3rdparty)
include_directories(${CMAKE_SOURCE_DIR}/../Common)
include_directories(${CMAKE_SOURCE_DIR}/../Include)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(ra_jni SHARED ${SOURCES})

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  target_compile_definitions(ra_jni PUBLIC -DSIMULATE)
endif()

set(OE_MIN_VERSION 0.12.0)
find_package(OpenEnclave ${OE_MIN_VERSION} CONFIG REQUIRED)

find_library(CRYPTO_LIB crypto)
find_library(SSL_LIB ssl)
target_link_libraries(ra_jni ${CRYPTO_LIB} ${SSL_LIB} mbedcrypto mbedtls openenclave::oehost)

install(TARGETS ra_jni DESTINATION lib)
