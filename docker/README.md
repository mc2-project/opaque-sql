Docker configuration for testing and development of Opaque.

### Hardware

To create and run a docker container in interactive mode for HARDWARE, use the following:


```shell
mv Dockerfile_hardware Dockerfile
docker build --tag opaque:1.0 .
```

You can then launch a docker container and test the opaque set up:

```shell
docker run --device /dev/sgx:/dev/sgx -it -m 4g -w opaque:1.0 

cd /root/opaque
source opaqueenv
source /opt/openenclave/share/openenclave/openenclaverc
build/sbt test
```

### Simulate

To create a docker image for Opaque with simulation mode:

```
mv Dockerfile_simulate Dockerfile
docker build --tag opaque:1.0 .
```

The above command will create the image and run the Opaque tests. You may need more memory than the default to pass all of them.

### Development

For development, you can mount your local Opaque source directory. You can run this only after creating the image from either the hardware or simulate step above:

```shell
docker run -it -m 4g -v $OPAQUE_HOME:/root/opaque -w /home/opaque \
	opaque
```

### Troubleshoot

If docker set up fails, go into the relevant docker file, and make sure that the file is pulling the latest Intel SGX DCAP driver in [step 2](https://github.com/openenclave/openenclave/blob/v0.9.x/docs/GettingStartedDocs/install_oe_sdk-Ubuntu_18.04.md) of OpenEnclave.