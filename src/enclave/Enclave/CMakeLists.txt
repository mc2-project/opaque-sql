cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveTrusted)

file(GLOB SOURCES "*.cpp")
set(SOURCES ${SOURCES}
  ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c
  ${CMAKE_CURRENT_SOURCE_DIR}/sgxaes_asm.o
  ${CMAKE_CURRENT_BINARY_DIR}/key.cpp)

add_custom_command(
  COMMAND ${SGX_EDGER8R} --trusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave --search-path "$ENV{SGX_SDK}/include"
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c)

add_custom_command(
  COMMAND ../ServiceProvider/main ${CMAKE_CURRENT_BINARY_DIR}/key.cpp
  DEPENDS ../ServiceProvider/main
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/key.cpp)

include_directories(SYSTEM "$ENV{SGX_SDK}/include/stlport")
include_directories(SYSTEM "$ENV{SGX_SDK}/include/tlibc")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -nostdinc -fvisibility=hidden -fpie -fstack-protector")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} -nostdinc++")
set(ENCLAVE_LINK_FLAGS "-Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined -Wl,-pie,-eenclave_entry -Wl,--export-dynamic -Wl,--defsym,__ImageBase=0 -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/Enclave.lds")

add_library(enclave.so SHARED ${SOURCES})
set_property(TARGET enclave.so PROPERTY POSITION_INDEPENDENT_CODE OFF)
set_target_properties(enclave.so PROPERTIES LINK_FLAGS ${ENCLAVE_LINK_FLAGS})

find_library(TRTS_LIB sgx_trts)
find_library(TRTS_SIM_LIB sgx_trts_sim)
find_library(TSTDC_LIB sgx_tstdc)
find_library(TSTDCXX_LIB sgx_tstdcxx)
find_library(TKEY_EXCHANGE_LIB sgx_tkey_exchange)
find_library(TCRYPTO_LIB sgx_tcrypto)
find_library(SERVICE_LIB sgx_tservice)
find_library(SERVICE_SIM_LIB sgx_tservice_sim)

if(NOT "$ENV{SGX_MODE}" EQUAL "HW")
  set(Trts_Library_Path "${TRTS_SIM_LIB}")
  set(Service_Library_Path "${SERVICE_SIM_LIB}")
else()
  set(Trts_Library_Path "${TRTS_LIB}")
  set(Service_Library_Path "${SERVICE_LIB}")
endif()

target_link_libraries(enclave.so -Wl,--whole-archive "${Trts_Library_Path}" -Wl,--no-whole-archive -Wl,--start-group "${TSTDC_LIB}" "${TSTDCXX_LIB}"
  "${TKEY_EXCHANGE_LIB}" "${TCRYPTO_LIB}" "${Service_Library_Path}" -Wl,--end-group)
