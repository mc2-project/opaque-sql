# Opaque demo

## Setup

Follow the setup instructions in README (steps 1 - 3) to launch a Spark shell with Opaque. In order to try out our attack, you should also include `--conf "spark.driver.extraJavaOptions=-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8001"` when you launch Spark shell.

## Usage example

### Data creation

```scala
val data = for (i <- 0 until 10) yield ("foo", i)
val rdd_data = spark.sparkContext.makeRDD(data, i)
```

### DataFrame creation
```
val words = spark.createDataFrame(rdd_data).toDF("word", "count")
val words_e = spark.createDataFrame(rdd_data).toDF("word", "count").encrypted
val words_o = spark.createDataFrame(rdd_data).toDF("word", "count").oblivious
```

### Query execution

```
words.filter($"count" > lit(3)).collect
words_e.filter($"count" > lit(3)).collect
words_o.filter($"count" > lit(3)).collect
```

## Attack example

### Attach jdb
`jdb -sourcepath $HOME/spark/sql/core/src/main/scala:$HOME/opaque/src/main/scala -attach 8000`

### Attack Spark SQL

`stop at org.apache.spark.sql.execution.FilterExec$$anonfun$12$$anonfun$apply$2:126
list
print row
cont
cont
cont
cont
print row
set r = false
clear org.apache.spark.sql.execution.FilterExec$$anonfun$12$$anonfun$apply$2:126
`

### Attack Opaque (works for both encryption and oblivious modes)
`stop at edu.berkeley.cs.rise.opaque.execution.ObliviousFilterExec$$anonfun$executeBlocked$14:323
list
dump filtered
set filtered[0] = 123
cont
`