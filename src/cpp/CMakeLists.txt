cmake_minimum_required(VERSION 3.15)

project(OpaqueEnclave)
enable_language(ASM)

option(FLATBUFFERS_LIB_DIR "Location of Flatbuffers library headers.")
option(FLATBUFFERS_GEN_CPP_DIR "Location of Flatbuffers generated C++ files.")

set(CMAKE_CXX_STANDARD 11)

set(OE_MIN_VERSION 0.12.0)
find_package(OpenEnclave ${OE_MIN_VERSION} CONFIG REQUIRED)

set(OE_CRYPTO_LIB
    mbed
    CACHE STRING "Crypto library used by enclaves.")

include_directories(app)
include_directories(common)
include_directories(enclave)
include_directories(service_provider)
include_directories(${FLATBUFFERS_LIB_DIR})
include_directories(${FLATBUFFERS_GEN_CPP_DIR})
include_directories(${OE_INCLUDEDIR})

if(CMAKE_SIZEOF_VOID_P EQUAL 4)
  set(ARCH_SIZE -m32)
else()
  set(ARCH_SIZE -m64)
endif()
set(COMMON_COMPILE_FLAGS ${ARCH_SIZE} -Wall -Wextra -Werror)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -DDEBUG -UNDEBUG -UEDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2 -DNDEBUG -DEDEBUG -UDEBUG")
set(CMAKE_CXX_FLAGS_PROFILE "${CMAKE_CXX_FLAGS_PROFILE} -O2 -DNDEBUG -DEDEBUG -UDEBUG -DPERF")

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  add_compile_definitions(SIMULATE)
endif()

add_custom_command(
  COMMAND openssl rsa -in $ENV{PRIVATE_KEY_PATH} -pubout -out $ENV{OPAQUE_HOME}/public_key.pub
  OUTPUT $ENV{OPAQUE_HOME}/public_key.pub)

add_custom_target(run ALL
    DEPENDS $ENV{OPAQUE_HOME}/public_key.pub)

add_subdirectory(app)
add_subdirectory(enclave)
add_subdirectory(service_provider)
