cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveJNI)

set(SOURCES
  App.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.c)

add_custom_command(
  COMMAND oeedger8r --untrusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.h ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.c ${CMAKE_CURRENT_BINARY_DIR}/Enclave_args.h)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(enclave_jni SHARED ${SOURCES})

target_include_directories(enclave_jni PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  target_compile_definitions(enclave_jni PUBLIC -DSIMULATE)
endif()

target_link_libraries(enclave_jni openenclave::oehost openenclave::oehostverify)

install(TARGETS enclave_jni DESTINATION lib)
