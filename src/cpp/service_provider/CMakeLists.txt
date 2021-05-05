cmake_minimum_required(VERSION 3.13)

project(OpaqueKeyGen)

set(SOURCES
    service_provider.cpp
    service_provider_jni.cpp
    agent_wget.cpp
    base64.c
    crypto.cpp
    ecp.cpp
    iasrequest.cpp
    sp_crypto.cpp)

link_directories(${OE_LIBDIR})
link_directories(${OE_LIBDIR}/openenclave/enclave)
include_directories(${OE_INCLUDEDIR})
include_directories(${OE_INCLUDEDIR}/openenclave/3rdparty)

add_library(ra_jni SHARED ${SOURCES})
target_compile_options(ra_jni PUBLIC ${COMMON_FLAGS} -fPIC -Wno-attributes)

find_library(CRYPTO_LIB crypto)
find_library(SSL_LIB ssl)
target_link_libraries(ra_jni ${CRYPTO_LIB} ${SSL_LIB} mbedcrypto mbedtls openenclave::oehost)

install(TARGETS ra_jni DESTINATION lib)
