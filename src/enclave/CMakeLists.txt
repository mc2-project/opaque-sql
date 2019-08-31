cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclave)

enable_language(ASM)

option(FLATBUFFERS_LIB_DIR "Location of Flatbuffers library headers.")
option(FLATBUFFERS_GEN_CPP_DIR "Location of Flatbuffers generated C++ files.")

if(NOT DEFINED ENV{SGX_SDK})
  message(FATAL_ERROR "$SGX_SDK environment variable must be set.")
endif()

include_directories(App)
include_directories(${CMAKE_BINARY_DIR}/App)
include_directories(Include)
include_directories(Common)
include_directories(Enclave)
include_directories(${CMAKE_BINARY_DIR}/Enclave)
include_directories(ServiceProvider)
include_directories(${FLATBUFFERS_LIB_DIR})
include_directories(${FLATBUFFERS_GEN_CPP_DIR})
include_directories("$ENV{SGX_SDK}/include")

if(CMAKE_SIZEOF_VOID_P EQUAL 4)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
  set(CMAKE_LIBRARY_PATH "${CMAKE_LIBRARY_PATH} $ENV{SGX_SDK}/lib")
  set(SGX_ENCLAVE_SIGNER "$ENV{SGX_SDK}/bin/x86/sgx_sign")
  set(SGX_EDGER8R "$ENV{SGX_SDK}/bin/x86/sgx_edger8r")
else()
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64")
  set(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} "$ENV{SGX_SDK}/lib64")
  set(SGX_ENCLAVE_SIGNER "$ENV{SGX_SDK}/bin/x64/sgx_sign")
  set(SGX_EDGER8R "$ENV{SGX_SDK}/bin/x64/sgx_edger8r")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra -Werror")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -DDEBUG -UNDEBUG -UEDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2 -DNDEBUG -DEDEBUG -UDEBUG")
set(CMAKE_CXX_FLAGS_PROFILE "${CMAKE_CXX_FLAGS_PROFILE} -O2 -DNDEBUG -DEDEBUG -UDEBUG -DPERF")

add_subdirectory(App)
add_subdirectory(Enclave)
add_subdirectory(ServiceProvider)
