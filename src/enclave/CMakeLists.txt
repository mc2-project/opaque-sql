cmake_minimum_required(VERSION 3.13)

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

include_directories(App)
include_directories(Common)
include_directories(Enclave)
include_directories(ServiceProvider)
include_directories(${FLATBUFFERS_LIB_DIR})
include_directories(${FLATBUFFERS_GEN_CPP_DIR})
include_directories(${OE_INCLUDEDIR})

set(COMMON_FLAGS -Wall -Wextra -Werror)

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  add_compile_definitions(SIMULATE)
endif()

add_custom_command(
  COMMAND openssl rsa -in $ENV{PRIVATE_KEY_PATH} -pubout -out $ENV{OPAQUE_HOME}/public_key.pub
  OUTPUT $ENV{OPAQUE_HOME}/public_key.pub)

add_custom_target(run ALL
    DEPENDS $ENV{OPAQUE_HOME}/public_key.pub)

add_subdirectory(App)
add_subdirectory(Enclave)
add_subdirectory(ServiceProvider)
