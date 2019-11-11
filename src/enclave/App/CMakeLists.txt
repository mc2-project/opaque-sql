cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveJNI)

set(SOURCES
  App.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.c)

add_custom_command(
  COMMAND ${OE_EDGER8R} --untrusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.h ${CMAKE_CURRENT_BINARY_DIR}/Enclave_u.c ${CMAKE_CURRENT_BINARY_DIR}/Enclave_args.h)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -Wno-attributes")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS}")

add_library(enclave_jni SHARED ${SOURCES})

# find_library(UKEY_EXCHANGE_LIB sgx_ukey_exchange)
# find_library(URTS_LIB sgx_urts)
# find_library(URTS_SIM_LIB sgx_urts_sim)
# find_library(UAE_SERVICE_LIB sgx_uae_service)
# find_library(UAE_SERVICE_SIM_LIB sgx_uae_service_sim)

# target_link_libraries(enclave_jni ${UKEY_EXCHANGE_LIB} pthread)
# if(NOT "$ENV{SGX_MODE}" STREQUAL "HW")
#   message(STATUS "Building for simulated SGX")
#   target_link_libraries(enclave_jni ${URTS_SIM_LIB} ${UAE_SERVICE_SIM_LIB})
# else()
#   message(STATUS "Building for SGX hardware")
#   target_link_libraries(enclave_jni ${URTS_LIB} ${UAE_SERVICE_LIB})
# endif()

target_include_directories(enclave_jni PRIVATE
  # Needed for the generated file helloworld_u.h
  ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(enclave_jni openenclave::oehostapp)

install(TARGETS enclave_jni DESTINATION lib)
