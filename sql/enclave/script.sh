SCALA_CP=$(pwd)/build:/usr/share/java/scala-library.jar
BASE_DIR=/home/wzheng/oblivious_computation/
SRC_DIR=/home/wzheng/oblivious_computation/src/main/scala/map_test/
ENCLAVE_DIR=/home/wzheng/oblivious_computation/enclave

cd $ENCLAVE_DIR
make clean; make SGX_PRERELEASE=1 SGX_MODE=HW
cd $BASE_DIR
rm libSGX.so enclave.signed.so
mv $ENCLAVE_DIR/libSGX.so ./
mv $ENCLAVE_DIR/enclave.signed.so ./

#scalac -d $BASE_DIR/build $SRC_DIR/TestMapPart.scala
#scala -Djava.library.path=$BASE_DIR -cp $SCALA_CP OSortPerfTest
