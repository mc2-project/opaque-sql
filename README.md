# Opaque for Apache Spark

How to build Opaque:

1. Edit the config files appropriately.
2. `source conf/spark-env.sh; build/sbt`
3. Within SBT: `sql/test:test-only org.apache.spark.sql.QEDSuite`
    You can prepend `~` to that to make it run continuously.
