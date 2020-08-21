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

link_directories("$ENV{OE_SDK_PATH}/lib/openenclave/enclave")
include_directories("$ENV{OE_SDK_PATH}/include")
include_directories("$ENV{OE_SDK_PATH}/include/openenclave/3rdparty")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(ra_jni SHARED ${SOURCES})

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  target_compile_definitions(ra_jni PUBLIC -DSIMULATE)
endif()

find_library(CRYPTO_LIB crypto)
find_library(SSL_LIB ssl)
target_link_libraries(ra_jni "${CRYPTO_LIB}" "${SSL_LIB}" mbedcrypto mbedtls openenclave::oehost openenclave::oehostverify)

install(TARGETS ra_jni DESTINATION lib)
