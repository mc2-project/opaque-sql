cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveTrusted)

file(GLOB SOURCES "*.cpp")
set(SOURCES ${SOURCES} ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c ${CMAKE_CURRENT_SOURCE_DIR}/sgxaes_asm.o)

add_custom_command(
  COMMAND ${SGX_EDGER8R} --trusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave --search-path "$ENV{SGX_SDK}/include"
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c)

include_directories(SYSTEM "$ENV{SGX_SDK}/include/stlport")
include_directories(SYSTEM "$ENV{SGX_SDK}/include/tlibc")

if(NOT $ENV{SGX_MODE} EQUAL "HW")
  set(Trts_Library_Name sgx_trts_sim)
  set(Service_Library_Name sgx_tservice_sim)
else()
  set(Trts_Library_Name sgx_trts)
  set(Service_Library_Name sgx_tservice)
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -nostdinc -fvisibility=hidden -fpie -fstack-protector")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} -nostdinc++")
set(ENCLAVE_LINK_FLAGS "-Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined -Wl,-pie,-eenclave_entry -Wl,--export-dynamic -Wl,--defsym,__ImageBase=0 -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/Enclave.lds")

find_library(TRTS_LIB ${Trts_Library_Name} "$ENV{SGX_SDK}/lib64")
find_library(TSTDC_LIB sgx_tstdc "$ENV{SGX_SDK}/lib64")
find_library(TSTDCXX_LIB sgx_tstdcxx "$ENV{SGX_SDK}/lib64")
find_library(TKEY_EXCHANGE_LIB sgx_tkey_exchange "$ENV{SGX_SDK}/lib64")
find_library(TCRYPTO_LIB sgx_tcrypto "$ENV{SGX_SDK}/lib64")
find_library(SERVICE_LIB ${Service_Library_Name} "$ENV{SGX_SDK}/lib64")

set(CMAKE_CXX_LINK_EXECUTABLE
  "${CMAKE_CXX_LINK_EXECUTABLE} -Wl,--whole-archive -ltrts -Wl,--no-whole-archive")
set(CMAKE_CXX_LINK_EXECUTABLE
  "${CMAKE_CXX_LINK_EXECUTABLE} -Wl,--start-group -lsgx_tstdc -lsgx_tstdcxx -lsgx_tkey_exchange -lsgx_tcrypto -l${Service_Library_Name} -Wl,--end-group")

add_library(enclave.so SHARED ${SOURCES})
set_property(TARGET enclave.so PROPERTY POSITION_INDEPENDENT_CODE OFF)
set_target_properties(enclave.so PROPERTIES LINK_FLAGS ${ENCLAVE_LINK_FLAGS})

target_link_libraries(enclave.so -Wl,--whole-archive "${TRTS_LIB}" -Wl,--no-whole-archive -Wl,--start-group "${TSTDC_LIB}" "${TSTDCXX_LIB}"
  "${TKEY_EXCHANGE_LIB}" "${TCRYPTO_LIB}" "${SERVICE_LIB}" -Wl,--end-group)
