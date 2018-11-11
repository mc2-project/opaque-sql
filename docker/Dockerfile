FROM ubuntu:xenial

RUN apt-get update && \
    apt-get install -y git wget build-essential openjdk-8-jdk-headless python cmake libssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN wget -O sgx_installer.bin https://download.01.org/intel-sgx/linux-2.3.1/ubuntu16.04/sgx_linux_x64_sdk_2.3.101.46683.bin && \
    chmod +x ./sgx_installer.bin && \
    echo $'no\n/usr/local' | ./sgx_installer.bin && \
    rm ./sgx_installer.bin

ENV SGX_SDK="/usr/local/sgxsdk"
ENV PATH="${PATH}:$SGX_SDK/bin:$SGX_SDK/bin/x64"
ENV PKG_CONFIG_PATH="${PKG_CONFIG_PATH}:$SGX_SDK/pkgconfig"
# Setting LD_LIBRARY_PATH seems not to work, so we instead just link each
# library into /usr/lib and run ldconfig. See
# https://stackoverflow.com/questions/51670836/saving-dockerfile-env-variables-for-future-use
RUN find $SGX_SDK/sdk_libs -name '*.so' -exec ln -s {} /usr/lib/ \; && ldconfig
# ENV LD_LIBRARY_PATH="${SGX_SDK}/sdk_libs"

RUN useradd -ms /bin/bash opaque
USER opaque
WORKDIR /home/opaque

RUN openssl ecparam -name prime256v1 -genkey -noout -out private_key.pem

RUN git clone https://github.com/ucbrise/opaque.git
WORKDIR /home/opaque/opaque

ENV SPARKSGX_DATA_DIR="/home/opaque/opaque/data"
ENV PRIVATE_KEY_PATH="/home/opaque/private_key.pem"

RUN build/sbt test:compile
