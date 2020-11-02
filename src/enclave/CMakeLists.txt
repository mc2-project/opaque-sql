cmake_minimum_required(VERSION 3.13)

project(OpaqueEnclave)

enable_language(ASM)

option(FLATBUFFERS_LIB_DIR "Location of Flatbuffers library headers.")
option(FLATBUFFERS_GEN_CPP_DIR "Location of Flatbuffers generated C++ files.")

find_package(OpenEnclave CONFIG REQUIRED)

include_directories(App)
include_directories(${CMAKE_BINARY_DIR}/App)
include_directories(Include)
include_directories(Common)
include_directories(Enclave)
include_directories(${CMAKE_BINARY_DIR}/Enclave)
include_directories(ServiceProvider)
include_directories(${FLATBUFFERS_LIB_DIR})
include_directories(${FLATBUFFERS_GEN_CPP_DIR})
include_directories("/opt/openenclave/include")

if(CMAKE_SIZEOF_VOID_P EQUAL 4)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32")
else()
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -DDEBUG -UNDEBUG -UEDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2 -DNDEBUG -DEDEBUG -UDEBUG")
set(CMAKE_CXX_FLAGS_PROFILE "${CMAKE_CXX_FLAGS_PROFILE} -O2 -DNDEBUG -DEDEBUG -UDEBUG -DPERF")

message("openssl rsa -in $ENV{OPAQUE_HOME}/private_key.pem -pubout -out $ENV{OPAQUE_HOME}/public_key.pub")
message("$ENV{OPAQUE_HOME}/public_key.pub")

add_custom_target(run ALL
    DEPENDS $ENV{OPAQUE_HOME}/public_key.pub)

add_custom_command(
  COMMAND openssl rsa -in $ENV{OPAQUE_HOME}/private_key.pem -pubout -out $ENV{OPAQUE_HOME}/public_key.pub
  OUTPUT $ENV{OPAQUE_HOME}/public_key.pub)

add_subdirectory(App)
add_subdirectory(Enclave)
add_subdirectory(ServiceProvider)
