Docker configuration for testing and development of Opaque.

### Hardware

To run the Opaque tests, use:

```shell
docker run -it -m 4g -w /home/opaque/opaque ankurdave/opaque build/sbt test
```

To launch an interactive console with Opaque, use:

```shell
docker run -it -m 4g -w /home/opaque/opaque ankurdave/opaque build/sbt console
```

For development, mount your local Opaque source directory and launch continuously-running tests against it:

```shell
docker run -it -m 4g -v $OPAQUE_HOME:/home/opaque/opaque -w /home/opaque/opaque \
    ankurdave/opaque build/sbt ~test
```

### Simulate

To create a docker image for Opaque with simulation mode, rename 'Dockerfile_simulate' to 'Dockerfile' (moving to a new folder if needed) and run:

```
docker build --tag opaque:1.0 .
```

The above command will create the image and run the Opaque tests. You may need more memory than the default to pass all of them.

For development, mount your local Opaque source directory. You need to run this after you create the image using the step above:

```shell
docker run -it -m 4g -v $OPAQUE_HOME:/root/opaque/opaque -w /home/opaque/opaque \
	opaque
```