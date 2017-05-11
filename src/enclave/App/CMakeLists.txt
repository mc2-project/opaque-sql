cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveJNI)

file(GLOB SOURCES "*.cpp")
set(SOURCES ${SOURCES} ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.c)

add_custom_command(
  COMMAND ${SGX_EDGER8R} --untrusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave --search-path "$ENV{SGX_SDK}/include"
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.c)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(libSGXEnclave.so SHARED ${SOURCES})

target_link_libraries(libSGXEnclave.so sgx_ukey_exchange pthread)
if(NOT $ENV{SGX_MODE} EQUAL "HW")
  target_link_libraries(libSGXEnclave.so sgx_urts_sim sgx_uae_service_sim)
else()
  target_link_libraries(libSGXEnclave.so sgx_urts sgx_uae_service)
endif()
