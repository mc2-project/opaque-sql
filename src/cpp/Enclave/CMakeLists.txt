cmake_minimum_required(VERSION 3.13)

project(OpaqueEnclaveTrusted)

set(SOURCES
  Aggregate.cpp
  Crypto.cpp
  Enclave.cpp
  Filter.cpp
  Flatbuffers.cpp
  FlatbuffersReaders.cpp
  FlatbuffersWriters.cpp
  NonObliviousSortMergeJoin.cpp
  BroadcastNestedLoopJoin.cpp
  Limit.cpp
  Project.cpp
  Sort.cpp
  sgxaes.cpp
  sgxaes_asm.S
  Random.cpp
  util.cpp
  ../Common/mCrypto.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c)

# Enclave code cannot use system STD libraries (contains unsupported operations), need to use OE's implementations
set(ENCLAVE_LINK_FLAGS "-Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined -Wl,-pie -Wl,--export-dynamic -Wl,--defsym,__ImageBase=0")

add_library(enclave_trusted SHARED ${SOURCES})
target_compile_options(enclave_trusted PUBLIC ${COMMON_FLAGS} -nostdinc -fvisibility=hidden -fpie -fstack-protector -nostdinc++)
target_compile_definitions(enclave_trusted PUBLIC OE_API_VERSION=2)
set_target_properties(enclave_trusted PROPERTIES
                      LINK_FLAGS
                      ${ENCLAVE_LINK_FLAGS}
                      POSITION_INDEPENDENT_CODE OFF)

# Need ${CMAKE_CURRENT_BINARY_DIR} for the generated file Enclave_t.h
target_include_directories(enclave_trusted 
                           PRIVATE 
                           ${CMAKE_CURRENT_BINARY_DIR} 
                           ${OE_INCLUDEDIR}/openenclave/3rdparty)

target_link_directories(enclave_trusted PRIVATE
                        ${OE_LIBDIR}
                        ${OE_LIBDIR}/openenclave/enclave)

target_link_libraries(enclave_trusted
                      openenclave::oeenclave
                      openenclave::oecrypto${OE_CRYPTO_LIB}
                      openenclave::oelibc
                      openenclave::oelibcxx
                      openenclave::oecore)

add_custom_command(
  COMMAND oeedger8r --trusted ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
          --search-path ${CMAKE_SOURCE_DIR}/Enclave
          --search-path ${OE_INCLUDEDIR}
          --search-path ${OE_INCLUDEDIR}/openenclave/edl/sgx
  DEPENDS ${CMAKE_SOURCE_DIR}/Enclave/Enclave.edl
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.h ${CMAKE_CURRENT_BINARY_DIR}/Enclave_t.c ${CMAKE_CURRENT_BINARY_DIR}/Enclave_args.h)

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
