cmake_minimum_required(VERSION 2.8)

project(OpaqueEnclaveTrusted)

set(SOURCES
  Aggregate.cpp
  Crypto.cpp
  Enclave.cpp
  Filter.cpp
  Flatbuffers.cpp
  FlatbuffersReaders.cpp
  FlatbuffersWriters.cpp
  IntegrityUtils.cpp
  Join.cpp
  Limit.cpp
  Project.cpp
  Sort.cpp
  sgxaes.cpp
  sgxaes_asm.S
  Random.cpp
  util.cpp
  ../Common/mCrypto.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c)

add_custom_command(
  COMMAND oeedger8r --trusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl --search-path ${CMAKE_SOURCE_DIR}/Enclave
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.h ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c ${CMAKE_CURRENT_BINARY_DIR}/Enclave_args.h)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -nostdinc -fvisibility=hidden -fpie -fstack-protector")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} -nostdinc++")
set(ENCLAVE_LINK_FLAGS "-Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined -Wl,-pie -Wl,--export-dynamic -Wl,--defsym,__ImageBase=0")

add_library(enclave_trusted SHARED ${SOURCES})
set_property(TARGET enclave_trusted PROPERTY POSITION_INDEPENDENT_CODE OFF)
set_target_properties(enclave_trusted PROPERTIES LINK_FLAGS ${ENCLAVE_LINK_FLAGS})

if ("$ENV{MODE}" STREQUAL "SIMULATE")
  target_compile_definitions(enclave_trusted PUBLIC -DSIMULATE)
endif()

target_compile_definitions(enclave_trusted PUBLIC OE_API_VERSION=2)

# Need for the generated file Enclave_t.h
target_include_directories(enclave_trusted PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(enclave_trusted 
                      openenclave::oeenclave 
                      openenclave::oelibc 
                      openenclave::oelibcxx
                      openenclave::oehostsock
		              openenclave::oehostresolver)

add_custom_command(
  COMMAND oesign sign -e $<TARGET_FILE:enclave_trusted> -c ${CMAKE_CURRENT_SOURCE_DIR}/Enclave.conf -k $ENV{PRIVATE_KEY_PATH} 
  DEPENDS enclave_trusted ${CMAKE_CURRENT_SOURCE_DIR}/Enclave.conf
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/enclave_trusted.signed)

# TODO: Use the user-generated private key to sign the enclave code.
# Currently we use the sample private key from the Intel SGX SDK.
add_custom_command(
  COMMAND mv ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted.so.signed  ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/enclave_trusted.signed
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so
)
add_custom_target(enclave_trusted_signed_target ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libenclave_trusted_signed.so DESTINATION lib)
