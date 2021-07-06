# Install OpenEnclave 0.12.0
echo 'deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu bionic main' | sudo tee /etc/apt/sources.list.d/intel-sgx.list
wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | sudo apt-key add -
echo "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-7 main" | sudo tee /etc/apt/sources.list.d/llvm-toolchain-bionic-7.list
wget -qO - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main" | sudo tee /etc/apt/sources.list.d/msprod.list
wget -qO - https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

sudo apt-get update
sudo apt-get -y install clang-7 libssl-dev gdb libsgx-enclave-common libsgx-enclave-common-dev libprotobuf10 libsgx-dcap-ql libsgx-dcap-ql-dev az-dcap-client open-enclave=0.12.0

# Install Opaque Dependencies
sudo apt-get -y install wget build-essential openjdk-8-jdk python libssl-dev libmbedtls-dev
pip3 install grpcio grpcio-tools # Needed for Pyspark listener

# Install a newer version of CMake (3.15)
wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh
sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local

# Install Spark 3.1.1
wget -nv https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
tar xvf spark-3.1.1*
sudo mkdir /opt/spark
sudo mv spark-3.1.1*/* /opt/spark
rm -rf spark-3.1.1*
sudo mkdir /opt/spark/work
sudo chmod -R a+wx /opt/spark/work

# Set Spark environment variables
export SPARK_SCALA_VERSION=2.12
export SPARK_HOME=/opt/spark
export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

# Set hostname to be localhost for Spark, since only running build/sbt test
# Fixes issues with assigning IP for sparkDriver on Github Actions VM
sudo hostname -b 127.0.0.1
touch /opt/spark/conf/spark-defaults.conf
echo "" >> /opt/spark/conf/spark-defaults.conf
echo "spark.driver.bindAddress  127.0.0.1" >> /opt/spark/conf/spark-defaults.conf

# Generate keypair for attestation
openssl genrsa -out ./private_key.pem -3 3072

# Set Opaque environment variables
source opaqueenv
source /opt/openenclave/share/openenclave/openenclaverc
export MODE=SIMULATE

build/sbt test
