cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveJNI)

set(SOURCES
  app.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/enclave_u.c)

add_library(enclave_jni SHARED ${SOURCES})
target_compile_options(enclave_jni PUBLIC ${COMMON_FLAGS} -fPIC -Wno-attributes)

# Need for the generated file Enclave_t.h
target_include_directories(enclave_jni PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(enclave_jni openenclave::oehost)

add_custom_command(
  COMMAND oeedger8r --untrusted ${CMAKE_SOURCE_DIR}/enclave/enclave.edl
          --search-path ${CMAKE_SOURCE_DIR}/enclave
          --search-path ${OE_INCLUDEDIR}
          --search-path ${OE_INCLUDEDIR}/openenclave/edl/sgx
  DEPENDS ${CMAKE_SOURCE_DIR}/enclave/enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/enclave_u.h ${CMAKE_CURRENT_BINARY_DIR}/enclave_u.c ${CMAKE_CURRENT_BINARY_DIR}/enclave_args.h)

install(TARGETS enclave_jni DESTINATION lib)
