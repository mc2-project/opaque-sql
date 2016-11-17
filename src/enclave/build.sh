#!/bin/bash

set -eu

SCALA_CP=$(pwd)/build:/usr/share/java/scala-library.jar
ENCLAVE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$ENCLAVE_DIR" && cd ../.. && pwd )"
BASE_DIR="$( cd "$ENCLAVE_DIR" && cd ../.. && pwd )"

cd $ENCLAVE_DIR
cp /opt/intel/sgxsdk/SampleCode/SampleEnclave/Enclave/Enclave_private.pem Enclave/
#make clean; make SGX_MODE=HW SGX_DEBUG=1
make clean; make SGX_MODE=HW SGX_PRERELEASE=1
#make clean; make SGX_DEBUG=1
rm -f $BASE_DIR/libSGXEnclave.so $BASE_DIR/enclave.signed.so
mv $ENCLAVE_DIR/libSGXEnclave.so $BASE_DIR
mv $ENCLAVE_DIR/enclave.signed.so $BASE_DIR
rm -f $BASE_DIR/libsample_libcrypto.so
cp $ENCLAVE_DIR/sample_libcrypto/libsample_libcrypto.so $BASE_DIR
rm -f $BASE_DIR/libservice_provider.so
mv $ENCLAVE_DIR/libservice_provider.so $BASE_DIR

#scalac -d $BASE_DIR/build $SRC_DIR/TestMapPart.scala
#scala -Djava.library.path=$BASE_DIR -cp $SCALA_CP OSortPerfTest
