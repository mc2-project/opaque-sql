cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveTrusted)

set(SOURCES
  Aggregate.cpp
  Crypto.cpp
  Enclave.cpp
  Filter.cpp
  Flatbuffers.cpp
  FlatbuffersReaders.cpp
  FlatbuffersWriters.cpp
  Join.cpp
  Project.cpp
  Sort.cpp
  sgxaes.cpp
  sgxaes_asm.S
  util.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c
  ${CMAKE_CURRENT_BINARY_DIR}/key.cpp)

add_custom_command(
  COMMAND ${SGX_EDGER8R} --trusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave --search-path "$ENV{SGX_SDK}/include"
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c)

add_custom_command(
  COMMAND ../ServiceProvider/keygen ${CMAKE_CURRENT_BINARY_DIR}/key.cpp
  DEPENDS ../ServiceProvider/keygen
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/key.cpp)

include_directories(SYSTEM "$ENV{SGX_SDK}/include/libcxx")
include_directories(SYSTEM "$ENV{SGX_SDK}/include/tlibc")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -nostdinc -fvisibility=hidden -fpie -fstack-protector")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} -nostdinc++")
set(ENCLAVE_LINK_FLAGS "-Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined -Wl,-pie,-eenclave_entry -Wl,--export-dynamic -Wl,--defsym,__ImageBase=0 -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/Enclave.lds")

add_library(enclave_trusted SHARED ${SOURCES})
set_property(TARGET enclave_trusted PROPERTY POSITION_INDEPENDENT_CODE OFF)
set_target_properties(enclave_trusted PROPERTIES LINK_FLAGS ${ENCLAVE_LINK_FLAGS})

find_library(TRTS_LIB sgx_trts)
find_library(TRTS_SIM_LIB sgx_trts_sim)
find_library(TSTDC_LIB sgx_tstdc)
find_library(TSTDCXX_LIB sgx_tcxx)
find_library(TKEY_EXCHANGE_LIB sgx_tkey_exchange)
find_library(TCRYPTO_LIB sgx_tcrypto)
find_library(SERVICE_LIB sgx_tservice)
find_library(SERVICE_SIM_LIB sgx_tservice_sim)

if(NOT "$ENV{SGX_MODE}" STREQUAL "HW")
  message(STATUS "Building for simulated SGX")
  set(Trts_Library_Path "${TRTS_SIM_LIB}")
  set(Service_Library_Path "${SERVICE_SIM_LIB}")
else()
  message(STATUS "Building for SGX hardware")
  set(Trts_Library_Path "${TRTS_LIB}")
  set(Service_Library_Path "${SERVICE_LIB}")
endif()

target_link_libraries(enclave_trusted -Wl,--whole-archive "${Trts_Library_Path}" -Wl,--no-whole-archive -Wl,--start-group "${TSTDC_LIB}" "${TSTDCXX_LIB}"
  "${TKEY_EXCHANGE_LIB}" "${TCRYPTO_LIB}" "${Service_Library_Path}" -Wl,--end-group)

add_custom_command(
  COMMAND ${SGX_ENCLAVE_SIGNER} sign -key "$ENV{SGX_SDK}/SampleCode/SampleEnclave/Enclave/Enclave_private.pem" -enclave $<TARGET_FILE:enclave_trusted> -out libenclave_trusted_signed.so -config ${CMAKE_CURRENT_SOURCE_DIR}/Enclave.config.xml
  DEPENDS enclave_trusted ${CMAKE_CURRENT_SOURCE_DIR}/Enclave.config.xml
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so)
add_custom_target(enclave_trusted_signed_target ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so DESTINATION lib)
