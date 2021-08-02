rpc
===

This directory contains all Scala files that have dependencies on the gRPC module.

**Note:** If any of these files are entered by the tests, there will be import exceptions. This is because our multi-partition tests *do not* include the fat jar when submitting to Spark using ``local-cluster``. This is to avoid introducing a dependency on ``build/sbt assembly`` for our test suite, since that will significantly slow it down.
