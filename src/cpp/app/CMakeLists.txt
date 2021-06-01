cmake_minimum_required(VERSION 3.15)

project(OpaqueEnclaveJNI)

# ---- Fetch opaque-utils ----
include(FetchContent)
FetchContent_Declare(
    opaque_utils_h
    GIT_REPOSITORY git@github.com:opaque-systems/opaque-utils.git
)
set(FETCHCONTENT_QUIET OFF)
set(HOST ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(opaque_utils_h)
# --------

set(SOURCES
  app.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/enclave_u.c)

add_library(enclave_jni SHARED ${SOURCES})
target_compile_options(enclave_jni PRIVATE ${COMMON_COMPILE_FLAGS} -fPIC -Wno-attributes)

# Need for the generated file Enclave_t.h
target_include_directories(enclave_jni PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(enclave_jni openenclave::oehost opaque_utils_h)

add_custom_command(
  COMMAND oeedger8r --untrusted ${CMAKE_SOURCE_DIR}/enclave/enclave.edl
          --search-path ${CMAKE_SOURCE_DIR}/enclave
          --search-path ${OE_INCLUDEDIR}
          --search-path ${OE_INCLUDEDIR}/openenclave/edl/sgx
  DEPENDS ${CMAKE_SOURCE_DIR}/enclave/enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/enclave_u.h ${CMAKE_CURRENT_BINARY_DIR}/enclave_u.c ${CMAKE_CURRENT_BINARY_DIR}/enclave_args.h)

install(TARGETS enclave_jni DESTINATION lib)
