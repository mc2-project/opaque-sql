# Using Ubuntu 18.04 image
FROM ubuntu:18.04

USER root

# Copy the current directory contents into the container
RUN mkdir -p /mc2/opaque-sql
COPY . /mc2/opaque-sql

# Install wget
RUN apt-get update
RUN apt-get install -y wget sudo gnupg2 git
RUN useradd -m docker && echo "docker:docker" | chpasswd && adduser docker sudo

# Install CMake
RUN cd /mc2 && \
    wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh && \
    sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local 

# Configure Intel and Microsoft APT repos
RUN echo 'deb [arch=amd64] https://download.01.org/intel-sgx/sgx_repo/ubuntu bionic main' | sudo tee /etc/apt/sources.list.d/intel-sgx.list && \
    wget -qO - https://download.01.org/intel-sgx/sgx_repo/ubuntu/intel-sgx-deb.key | sudo apt-key add - && \
    echo "deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-7 main" | sudo tee /etc/apt/sources.list.d/llvm-toolchain-bionic-7.list && \
    wget -qO - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add - && \
    echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main" | sudo tee /etc/apt/sources.list.d/msprod.list && \
    wget -qO - https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add - && \
    sudo apt update

# Install Intel and Open Enclave packages and dependencies
RUN sudo apt -y install clang-8 libssl-dev gdb libsgx-enclave-common libsgx-quote-ex libprotobuf10 libsgx-dcap-ql libsgx-dcap-ql-dev az-dcap-client open-enclave=0.12.0

# Install SBT dependencies
RUN sudo apt -y install build-essential openjdk-8-jdk python libssl-dev libmbedtls-dev

# Install Spark 3.1.1
RUN wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz && \
    tar xvf spark-3.1.1* && \
    sudo mkdir -p /opt/spark && \
    sudo mv spark-3.1.1*/* /opt/spark && \
    rm -rf spark-3.1.1* && \
    sudo mkdir -p /opt/spark/work && \
    sudo chmod -R a+wx /opt/spark/work

# Set Spark environment variables in bashrc
RUN echo "" >> ~/.bashrc && \
    echo "# Spark settings" >> ~/.bashrc && \
    echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc && \
    echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.bashrc && \
    echo "" >> ~/.bashrc

# Source Open Enclave on every login
RUN echo "source /opt/openenclave/share/openenclave/openenclaverc" >> ~/.bashrc

# Set environment variables
ENV OPAQUE_HOME="/mc2/opaque-sql"
ENV OPAQUE_DATA_DIR=${OPAQUE_HOME}/data/
ENV SPARK_SCALA_VERSION=2.12
ENV SYMMETRIC_KEY_PATH=${OPAQUE_HOME}/symmetric_key.key
ENV PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem
ENV MODE=SIMULATE
ENV OE_SDK_PATH=/opt/openenclave/

# Build Opaque SQL
SHELL ["/bin/bash", "-c"]
RUN cd /mc2/opaque-sql && source /opt/openenclave/share/openenclave/openenclaverc && build/sbt keys
RUN cd /mc2/opaque-sql && source /opt/openenclave/share/openenclave/openenclaverc && build/sbt package

# Set the working directory to the Opaque SQL directory
WORKDIR /mc2/opaque-sql

# Expose ports
EXPOSE 22
EXPOSE 50051-50055
