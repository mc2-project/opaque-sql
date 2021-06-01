cmake_minimum_required(VERSION 3.15)

project(OpaqueEnclaveTrusted)

set(SOURCES
  crypto/ks_crypto.cpp
  crypto/sgxaes.cpp
  crypto/sgxaes_asm.S
  flatbuffer_helpers/flatbuffers.cpp
  flatbuffer_helpers/flatbuffers_readers.cpp
  flatbuffer_helpers/flatbuffers_writers.cpp
  physical_operators/aggregate.cpp
  physical_operators/broadcast_nested_loop_join.cpp
  physical_operators/filter.cpp
  physical_operators/limit.cpp
  physical_operators/non_oblivious_sort_merge_join.cpp
  physical_operators/project.cpp
  physical_operators/sort.cpp
  enclave.cpp
  util.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/enclave_t.c)

add_library(enclave_trusted SHARED ${SOURCES})

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  target_compile_definitions(enclave_trusted PUBLIC -DSIMULATE)
endif()

if ("$ENV{DEBUG}" EQUAL 1)
  target_compile_options(enclave_trusted PUBLIC -g -O0)
  target_compile_definitions(enclave_trusted PUBLIC -DDEBUG)
endif()

add_compile_options(${COMMON_COMPILE_FLAGS} -fvisibility=hidden -fpie -fstack-protector -Wno-unused-parameter -Wno-unused-variable -Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined -Wl,-pie -Wl,--export-dynamic -Wl,--defsym,__ImageBase=0)
target_compile_definitions(enclave_trusted PUBLIC OE_API_VERSION=2)
set_target_properties(enclave_trusted PROPERTIES POSITION_INDEPENDENT_CODE OFF)

########## Fetch and make opaque-utils library available ##########
include(FetchContent)
FetchContent_Declare(
  opaque_utils_e
  GIT_REPOSITORY git@github.com:opaque-systems/opaque-utils.git
)
set(FETCHCONTENT_QUIET OFF)
set(HOST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(opaque_utils_e)
add_compile_definitions(SPDLOG_NO_THREAD_ID FMT_USE_INT128=0)

include_directories(${spdlog_SOURCE_DIR}/include)
##################################################################

# Need ${CMAKE_CURRENT_BINARY_DIR} for the generated file enclave_t.h
target_include_directories(enclave_trusted 
                           PRIVATE 
                           ${CMAKE_CURRENT_BINARY_DIR} 
                           ${OE_INCLUDEDIR}/openenclave/3rdparty)

target_link_directories(enclave_trusted PRIVATE
                        ${OE_LIBDIR}
                        ${OE_LIBDIR}/openenclave/enclave)

target_link_libraries(enclave_trusted
                      opaque_utils_e
                      openenclave::oeenclave
                      openenclave::oecrypto${OE_CRYPTO_LIB}
                      openenclave::oelibc
                      openenclave::oelibcxx
                      openenclave::oecore)

add_custom_command(
  COMMAND oeedger8r --trusted ${CMAKE_SOURCE_DIR}/enclave/enclave.edl
          --search-path ${CMAKE_SOURCE_DIR}/enclave
          --search-path ${OE_INCLUDEDIR}
          --search-path ${OE_INCLUDEDIR}/openenclave/edl/sgx
  DEPENDS ${CMAKE_SOURCE_DIR}/enclave/enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/enclave_t.h ${CMAKE_CURRENT_BINARY_DIR}/enclave_t.c ${CMAKE_CURRENT_BINARY_DIR}/enclave_args.h)

add_custom_command(
  COMMAND openenclave::oesign sign -e $<TARGET_FILE:enclave_trusted> -c ${CMAKE_CURRENT_SOURCE_DIR}/Enclave.conf -k $ENV{PRIVATE_KEY_PATH} 
  DEPENDS enclave_trusted ${CMAKE_CURRENT_SOURCE_DIR}/Enclave.conf
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/enclave_trusted.signed)

add_custom_command(
  COMMAND mv ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted.so.signed  ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/enclave_trusted.signed
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so
)

add_custom_target(enclave_trusted_signed_target ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so DESTINATION lib)
