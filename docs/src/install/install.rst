*************************
Installation
*************************

After downloading the Opaque codebase, build and test it as follows.

1. Install dependencies and the [OpenEnclave SDK](https://github.com/openenclave/openenclave/blob/v0.12.0/docs/GettingStartedDocs/install_oe_sdk-Ubuntu_18.04.md). We currently support OE version 0.12.0 (so please install with ``open-enclave=0.12.0``) and Ubuntu 18.04.

   .. code-block:: bash
               
                   # For Ubuntu 18.04:
                   sudo apt install wget build-essential openjdk-8-jdk python libssl-dev

                   # Install a newer version of CMake (>= 3.13)
                   wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh
                   sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local

2. On the master, generate a keypair using OpenSSL for remote attestation.

   .. code-block:: bash
               
                   openssl genrsa -out private_key.pem -3 3072

3. Change into the Opaque root directory and edit Opaque's environment variables in ``opaqueenv`` if desired. Export Opaque and OpenEnclave environment variables via

   .. code-block:: bash
                   
                   source opaqueenv
                   source /opt/openenclave/share/openenclave/openenclaverc

   By default, Opaque runs in hardware mode (environment variable ``MODE=HARDWARE``).
   If you do not have a machine with a hardware enclave but still wish to test out Opaque's functionality locally, then set ``export MODE=SIMULATE``.

4. Run the Opaque tests:

   .. code-block:: bash
                
                   cd ${OPAQUE_HOME}
                   build/sbt test
