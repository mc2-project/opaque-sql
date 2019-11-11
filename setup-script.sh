#!/usr/bin/env bash
OPAQUE_HOME=/home/vitikoo/my_code/opaque
source ~/sgxsdk/environment
# create keypair on master for remote attestation
# DO WE NEED THIS WITH OE SDK??
# cd ${OPAQUE_HOME}
#openssl ecparam -name prime256v1 -genkey -noout -out private_key.pem
export SPARKSGX_DATA_DIR=${OPAQUE_HOME}/data
export PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem
export SGX_MODE=HW

export SPARK_HOME=/home/vitikoo/spark-2.4.4-bin-hadoop2.7/

export OE_SDK_PATH=/opt/openenclave