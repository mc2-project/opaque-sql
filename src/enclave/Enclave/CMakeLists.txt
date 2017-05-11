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
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdinc++")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-undefined")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -nostdlib -nodefaultlibs -nostartfiles")
set(CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--whole-archive -l${Trts_Library_Name} -Wl,--no-whole-archive")
set(CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--start-group -lsgx_tstdc -lsgx_tstdcxx -lsgx_tkey_exchange -lsgx_tcrypto -l${Service_Library_Name} -Wl,--end-group")
set(CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined")
set(CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -Wl,-pie,-eenclave_entry -Wl,--export-dynamic")
set(CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--defsym,__ImageBase=0")
set(CMAKE_SHARED_LINKER_FLAGS
  "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/Enclave.lds")

add_library(enclave.so SHARED ${SOURCES})
set_property(TARGET enclave.so PROPERTY POSITION_INDEPENDENT_CODE OFF)
