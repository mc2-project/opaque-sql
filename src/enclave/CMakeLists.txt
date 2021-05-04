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
include_directories(Include)
include_directories(Common)
include_directories(Enclave)
include_directories(ServiceProvider)
include_directories(${FLATBUFFERS_LIB_DIR})
include_directories(${FLATBUFFERS_GEN_CPP_DIR})
include_directories(${OE_INCLUDEDIR})

if(CMAKE_SIZEOF_VOID_P EQUAL 4)
  set(EXEC_SIZE "")
else()
  set(EXEC_SIZE "")
endif()

set(COMMON_FLAGS ${EXEC_SIZE} -Wall -Wextra -Werror)

add_custom_target(run ALL
    DEPENDS $ENV{OPAQUE_HOME}/public_key.pub)

add_custom_command(
  COMMAND openssl rsa -in $ENV{PRIVATE_KEY_PATH} -pubout -out $ENV{OPAQUE_HOME}/public_key.pub
  OUTPUT $ENV{OPAQUE_HOME}/public_key.pub)

add_subdirectory(App)
add_subdirectory(Enclave)
add_subdirectory(ServiceProvider)
