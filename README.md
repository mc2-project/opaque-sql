# Opaque for Apache Spark

How to build Opaque:

1. Edit the config files appropriately.
2. `source conf/spark-env.sh; build/sbt`
3. Within SBT: `sql/test:test-only org.apache.spark.sql.QEDSuite`
    You can prepend `~` to that to make it run continuously.

How to generate benchmark results for one node:

```
source conf/spark-env.sh

export SGX_MODE=HW  # if running in HW mode - for logging

SGX_PERF=1 build/sbt assembly && \
SGX_PERF=1 bin/spark-submit \
  --conf spark.ui.showConsoleProgress=false \
  --master local[1] \
  --class org.apache.spark.sql.QEDBenchmark \
  assembly/target/scala-2.11/spark-assembly-2.0.0-SNAPSHOT-hadoop2.2.0.jar 2>&1 \
  | tee ~/bench-$(git rev-parse --short HEAD)-$SGX_MODE.txt

grep -P '^(non-)?oblivious sort:|\}$' ~/bench-$(git rev-parse --short HEAD)-$SGX_MODE.txt \
  | perl -pe 's/^(non-)?oblivious sort: ([0-9.]+) ms$/$2/' \
  | perl -ne 'if (!/^\{/) { $time += $_ } else { s/\}$/, "sort time": $time}/; print; $time = 0 }'
```
