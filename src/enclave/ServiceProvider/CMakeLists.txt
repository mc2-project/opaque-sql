cmake_minimum_required(VERSION 2.8)

project(OpaqueKeyGen)

set(SOURCES
  ServiceProviderJNI.cpp
  KeyGen.cpp
  ecp.cpp
  ServiceProvider.cpp
  sp_crypto.cpp
  iasrequest.cpp
  agent_wget.cpp
  crypto.cpp
  base64.c)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(ra_jni SHARED ${SOURCES})

find_library(CRYPTO_LIB crypto)
find_library(SSL_LIB ssl)
target_link_libraries(ra_jni "${CRYPTO_LIB}" "${SSL_LIB}")

add_executable(keygen ${SOURCES})
target_link_libraries(keygen "${CRYPTO_LIB}" "${SSL_LIB}")

install(TARGETS ra_jni DESTINATION lib)
