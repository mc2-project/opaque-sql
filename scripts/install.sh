#!/bin/bash

# This is an installation script that will automatically install all Opaque SQL dependencies (including Spark).
# Compatible with Ubuntu 18.04.

# Install the Open Enclave SDK
# Instructions from https://github.com/openenclave/openenclave/blob/v0.12.0/docs/GettingStartedDocs/install_oe_sdk-Ubuntu_18.04.md
echo 'deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu bionic main' | sudo tee /etc/apt/sources.list.d/intel-sgx.list
wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | sudo apt-key add -

echo "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-7 main" | sudo tee /etc/apt/sources.list.d/llvm-toolchain-bionic-7.list
wget -qO - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -

echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main" | sudo tee /etc/apt/sources.list.d/msprod.list
wget -qO - https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

sudo apt update

sudo apt -y install dkms
wget https://download.01.org/intel-sgx/sgx-dcap/1.9/linux/distro/ubuntu18.04-server/sgx_linux_x64_driver_1.36.2.bin
chmod +x sgx_linux_x64_driver_1.36.2.bin
sudo ./sgx_linux_x64_driver_1.36.2.bin
rm -rf sgx_linux_x64_driver_1.36.2.bin

sudo apt -y install clang-7 libssl-dev gdb libsgx-enclave-common libprotobuf10 libsgx-dcap-ql libsgx-dcap-ql-dev az-dcap-client open-enclave=0.12.0

# Install SBT dependencies
sudo apt -y install wget build-essential openjdk-8-jdk python libssl-dev libmbedtls-dev
pip3 install grpcio grpcio-tools # Needed for Pyspark listener

# Install a newer version of CMake (3.15)
wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh
sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local
rm cmake-3.15.6-Linux-x86_64.sh


# Install Spark 3.1.1
# Note that this install in the /opt/ directory.
# Some systems may already have Spark installed: note that
# the environment variables in the following block will
# need to be set accordingly if so.
wget -nv https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
tar xvf spark-3.1.1*
sudo mkdir /opt/spark
sudo mv spark-3.1.1*/* /opt/spark
rm -rf spark-3.1.1*
sudo mkdir /opt/spark/work
sudo chmod -R a+wx /opt/spark/work

# Source necessary environment variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";
OPAQUE_DIR="${SCRIPT_DIR%/*}"
cd ${OPAQUE_DIR}
source opaqueenv
source /opt/openenclave/share/openenclave/openenclaverc
