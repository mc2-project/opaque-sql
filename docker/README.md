Docker configuration for testing and development of Opaque.

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
