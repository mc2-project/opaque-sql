SCALA_CP=$(pwd)/build:/usr/share/java/scala-library.jar
BASE_DIR=/home/wzheng/sparksgx
SRC_DIR=/home/wzheng/sparksgx
ENCLAVE_DIR=/home/wzheng/sparksgx/sql/enclave

cd $ENCLAVE_DIR
make clean; make SGX_MODE=HW SGX_DEBUG=1
rm $BASE_DIR/libSGXEnclave.so $BASE_DIR/enclave.signed.so
mv $ENCLAVE_DIR/libSGXEnclave.so $BASE_DIR
mv $ENCLAVE_DIR/enclave.signed.so $BASE_DIR

#scalac -d $BASE_DIR/build $SRC_DIR/TestMapPart.scala
#scala -Djava.library.path=$BASE_DIR -cp $SCALA_CP OSortPerfTest
