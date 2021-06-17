.. _functionalities:

*************************
Supported functionalities
*************************

This section lists Opaque's supported functionalities, which is a subset of that of Spark SQL. The syntax for these functionalities is the same as Spark SQL -- Opaque simply replaces the execution to work with encrypted data.

SQL interface
#############

Data types
**********

Out of the existing `Spark SQL types <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_, Opaque supports

- All numeric types. ``DecimalType`` is supported via conversion into ``FloatType``
- ``StringType``
- ``BinaryType``
- ``BooleanType``
- ``TimestampTime``, ``DateType``
- ``ArrayType``, ``MapType``

Functions
*********

We currently support a subset of the Spark SQL functions, including both scalar and aggregate-like functions.

- Scalar functions: ``case``, ``cast``, ``concat``, ``contains``, ``if``, ``in``, ``like``, ``substring``, ``upper``
- Aggregate functions: ``average``, ``count``, ``first``, ``last``, ``max``, ``min``, ``sum``

UDFs are not supported directly, but one can :ref:`extend Opaque with additional functions <udfs>` by writing it in C++.


Operators
*********

Opaque supports the core SQL operators:

- Projection (e.g., ``SELECT`` statements)
- Filter
- Global aggregation and grouping aggregation
- Order by, sort by
- All join types except: cross join, full outer join, existence join
- Limit

DataFrame interface
###################

Because Opaque SQL only replaces physical operators to work with encrypted data, the DataFrame interface is exactly the same as Spark's both for `Scala <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html>`_ and `Python <https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html?highlight=dataframe#pyspark.sql.DataFrame>`_. Opaque SQL is still a work in progress, so not all of these functionalities are currently implemented. See below for a complete list in Scala.

Supported operations
********************

Actions
-------
- `collect(): Array[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#collect():Array[T]>`_
- `collectAsList(): List[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#collectAsList():java.util.List[T]>`_
- `count(): Long <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#count():Long>`_
- `first(): T <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#first():T>`_
- `foreach(func: ForeachFunction[T]): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#foreach(func:org.apache.spark.api.java.function.ForeachFunction[T]):Unit>`_
- `foreach(f: T => Unit): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#foreach(f:T=%3EUnit):Unit>`_
- `foreachPartition(func: ForeachPartitionFunction[T]): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#foreachPartition(func:org.apache.spark.api.java.function.ForeachPartitionFunction[T]):Unit>`_
- `foreachPartition(f: Iterator[T] => Unit): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#foreachPartition(f:Iterator[T]=%3EUnit):Unit>`_
- `head(): T <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#head():T>`_
- `head(n: Int): Array[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#head(n:Int):Array[T]>`_
- `show(numRows: Int, truncate: Int, vertical: Boolean): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#show(numRows:Int,truncate:Int,vertical:Boolean):Unit>`_
- `show(numRows: Int, truncate: Int): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#show(numRows:Int,truncate:Int):Unit>`_
- `show(numRows: Int, truncate: Boolean): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#show(numRows:Int,truncate:Boolean):Unit>`_
- `show(truncate: Boolean): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#show(truncate:Boolean):Unit>`_
- `show(): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#show():Unit>`_
- `show(numRows: Int): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#show(numRows:Int):Unit>`_
- `take(n: Int): Array[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#take(n:Int):Array[T]>`_
- `takeAsList(n: Int): List[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#takeAsList(n:Int):java.util.List[T]>`_
- `toLocalIterator(): Iterator[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#toLocalIterator():java.util.Iterator[T]>`_

Basic Dataset functions
-----------------------
- `cache(): Dataset.this.type <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#cache():Dataset.this.type>`_
- `columns: Array[String] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#columns:Array[String]>`_
- `createGlobalTempView(viewName: String): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#createGlobalTempView(viewName:String):Unit>`_
- `createOrReplaceGlobalTempView(viewName: String): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#createOrReplaceGlobalTempView(viewName:String):Unit>`_
- `createOrReplaceTempView(viewName: String): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#createOrReplaceTempView(viewName:String):Unit>`_
- `createTempView(viewName: String): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#createTempView(viewName:String):Unit>`_
- `dtypes: Array[(String, String)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#dtypes:Array[(String,String)]>`_
- `explain(): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#explain():Unit>`_
- `explain(extended: Boolean): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#explain(extended:Boolean):Unit>`_
- `explain(mode: String): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#explain(mode:String):Unit>`_
- `hint(name: String, parameters: Any*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#hint(name:String,parameters:Any*):org.apache.spark.sql.Dataset[T]>`_
- `inputFiles: Array[String] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#inputFiles:Array[String]>`_
- `isEmpty: Boolean <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#isEmpty:Boolean>`_
- `isLocal: Boolean <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#isLocal:Boolean>`_
- `javaRDD: JavaRDD[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#javaRDD:org.apache.spark.api.java.JavaRDD[T]>`_
- `localCheckpoint(eager: Boolean): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#localCheckpoint(eager:Boolean):org.apache.spark.sql.Dataset[T]>`_
- `localCheckpoint(): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#localCheckpoint():org.apache.spark.sql.Dataset[T]>`_
- `printSchema(level: Int): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#printSchema(level:Int):Unit>`_
- `printSchema(): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#printSchema():Unit>`_
- `rdd: org.apache.spark.rdd.RDD[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#rdd:org.apache.spark.rdd.RDD[T]>`_
- `schema: types.StructType <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#schema:org.apache.spark.sql.types.StructType>`_
- `storageLevel: org.apache.spark.storage.StorageLevel <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#storageLevel:org.apache.spark.storage.StorageLevel>`_
- `toDF(colNames: String*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#toDF(colNames:String*):org.apache.spark.sql.DataFrame>`_
- `toDF(): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#toDF():org.apache.spark.sql.DataFrame>`_
- `toJavaRDD: JavaRDD[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#toJavaRDD:org.apache.spark.api.java.JavaRDD[T]>`_
- `unpersist(): Dataset.this.type <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#unpersist():Dataset.this.type>`_
- `unpersist(blocking: Boolean): Dataset.this.type <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#unpersist(blocking:Boolean):Dataset.this.type>`_
- `write: DataFrameWriter[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#write:org.apache.spark.sql.DataFrameWriter[T]>`_
- `writeStream: streaming.DataStreamWriter[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#writeStream:org.apache.spark.sql.streaming.DataStreamWriter[T]>`_
- `writeTo(table: String): DataFrameWriterV2[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#writeTo(table:String):org.apache.spark.sql.DataFrameWriterV2[T]>`_
- `registerTempTable(tableName: String): Unit <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#registerTempTable(tableName:String):Unit>`_

Streaming
---------
- `isStreaming: Boolean <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#isStreaming:Boolean>`_
- `withWatermark(eventTime: String, delayThreshold: String): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#withWatermark(eventTime:String,delayThreshold:String):org.apache.spark.sql.Dataset[T]>`_

Typed transformations
---------------------
- `alias(alias: Symbol): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#alias(alias:Symbol):org.apache.spark.sql.Dataset[T]>`_
- `alias(alias: String): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#alias(alias:String):org.apache.spark.sql.Dataset[T]>`_
- `as(alias: Symbol): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#as(alias:Symbol):org.apache.spark.sql.Dataset[T]>`_
- `as(alias: String): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#as(alias:String):org.apache.spark.sql.Dataset[T]>`_
- `coalesce(numPartitions: Int): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#coalesce(numPartitions:Int):org.apache.spark.sql.Dataset[T]>`_
- `distinct(): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#distinct():org.apache.spark.sql.Dataset[T]>`_
- `dropDuplicates(col1: String, cols: String*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#dropDuplicates(col1:String,cols:String*):org.apache.spark.sql.Dataset[T]>`_
- `dropDuplicates(colNames: Array[String]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#dropDuplicates(colNames:Array[String]):org.apache.spark.sql.Dataset[T]>`_
- `dropDuplicates(colNames: Seq[String]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#dropDuplicates(colNames:Seq[String]):org.apache.spark.sql.Dataset[T]>`_
- `dropDuplicates(): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#dropDuplicates():org.apache.spark.sql.Dataset[T]>`_
- `filter(func: FilterFunction[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#filter(func:org.apache.spark.api.java.function.FilterFunction[T]):org.apache.spark.sql.Dataset[T]>`_
- `filter(func: T => Boolean): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#filter(func:T=%3EBoolean):org.apache.spark.sql.Dataset[T]>`_
- `filter(conditionExpr: String): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#filter(conditionExpr:String):org.apache.spark.sql.Dataset[T]>`_
- `filter(condition: Column): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#filter(condition:org.apache.spark.sql.Column):org.apache.spark.sql.Dataset[T]>`_
- `flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#flatMap[U](f:org.apache.spark.api.java.function.FlatMapFunction[T,U],encoder:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `flatMap[U](func: T => TraversableOnce[U])(implicitevidence: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#flatMap[U](func:T=%3ETraversableOnce[U])(implicitevidence$8:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K]): KeyValueGroupedDataset[K, T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#groupByKey[K](func:org.apache.spark.api.java.function.MapFunction[T,K],encoder:org.apache.spark.sql.Encoder[K]):org.apache.spark.sql.KeyValueGroupedDataset[K,T]>`_
- `groupByKey[K](func: T => K)(implicitevidence: Encoder[K]): KeyValueGroupedDataset[K, T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#groupByKey[K](func:T=%3EK)(implicitevidence$3:org.apache.spark.sql.Encoder[K]):org.apache.spark.sql.KeyValueGroupedDataset[K,T]>`_
- `joinWith[U](other: Dataset[U], condition: Column): Dataset[(T, U)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#joinWith[U](other:org.apache.spark.sql.Dataset[U],condition:org.apache.spark.sql.Column):org.apache.spark.sql.Dataset[(T,U)]>`_
- `joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#joinWith[U](other:org.apache.spark.sql.Dataset[U],condition:org.apache.spark.sql.Column,joinType:String):org.apache.spark.sql.Dataset[(T,U)]>`_
- `limit(n: Int): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#limit(n:Int):org.apache.spark.sql.Dataset[T]>`_
- `map[U](func: MapFunction[T, U], encoder: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#map[U](func:org.apache.spark.api.java.function.MapFunction[T,U],encoder:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `map[U](func: T => U)(implicitevidence: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#map[U](func:T=%3EU)(implicitevidence$6:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#mapPartitions[U](f:org.apache.spark.api.java.function.MapPartitionsFunction[T,U],encoder:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `mapPartitions[U](func: Iterator[T] => Iterator[U])(implicitevidence: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#mapPartitions[U](func:Iterator[T]=%3EIterator[U])(implicitevidence$7:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `orderBy(sortExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#orderBy(sortExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `orderBy(sortCol: String, sortCols: String*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#orderBy(sortCol:String,sortCols:String*):org.apache.spark.sql.Dataset[T]>`_
- `randomSplit(weights: Array[Double]): Array[Dataset[T]] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#randomSplit(weights:Array[Double]):Array[org.apache.spark.sql.Dataset[T]]>`_
- `randomSplit(weights: Array[Double], seed: Long): Array[Dataset[T]] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#randomSplit(weights:Array[Double],seed:Long):Array[org.apache.spark.sql.Dataset[T]]>`_
- `randomSplitAsList(weights: Array[Double], seed: Long): List[Dataset[T]] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#randomSplitAsList(weights:Array[Double],seed:Long):java.util.List[org.apache.spark.sql.Dataset[T]]>`_
- `repartition(partitionExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#repartition(partitionExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#repartition(numPartitions:Int,partitionExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `repartition(numPartitions: Int): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#repartition(numPartitions:Int):org.apache.spark.sql.Dataset[T]>`_
- `repartitionByRange(partitionExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#repartitionByRange(partitionExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#repartitionByRange(numPartitions:Int,partitionExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `select[U1, U2, U3, U4, U5](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3], c4: TypedColumn[T, U4], c5: TypedColumn[T, U5]): Dataset[(U1, U2, U3, U4, U5)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select[U1,U2,U3,U4,U5](c1:org.apache.spark.sql.TypedColumn[T,U1],c2:org.apache.spark.sql.TypedColumn[T,U2],c3:org.apache.spark.sql.TypedColumn[T,U3],c4:org.apache.spark.sql.TypedColumn[T,U4],c5:org.apache.spark.sql.TypedColumn[T,U5]):org.apache.spark.sql.Dataset[(U1,U2,U3,U4,U5)]>`_
- `select[U1, U2, U3, U4](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3], c4: TypedColumn[T, U4]): Dataset[(U1, U2, U3, U4)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select[U1,U2,U3,U4](c1:org.apache.spark.sql.TypedColumn[T,U1],c2:org.apache.spark.sql.TypedColumn[T,U2],c3:org.apache.spark.sql.TypedColumn[T,U3],c4:org.apache.spark.sql.TypedColumn[T,U4]):org.apache.spark.sql.Dataset[(U1,U2,U3,U4)]>`_
- `select[U1, U2, U3](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2], c3: TypedColumn[T, U3]): Dataset[(U1, U2, U3)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select[U1,U2,U3](c1:org.apache.spark.sql.TypedColumn[T,U1],c2:org.apache.spark.sql.TypedColumn[T,U2],c3:org.apache.spark.sql.TypedColumn[T,U3]):org.apache.spark.sql.Dataset[(U1,U2,U3)]>`_
- `select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select[U1,U2](c1:org.apache.spark.sql.TypedColumn[T,U1],c2:org.apache.spark.sql.TypedColumn[T,U2]):org.apache.spark.sql.Dataset[(U1,U2)]>`_
- `select[U1](c1: TypedColumn[T, U1]): Dataset[U1] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select[U1](c1:org.apache.spark.sql.TypedColumn[T,U1]):org.apache.spark.sql.Dataset[U1]>`_
- `sort(sortExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sort(sortExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `sort(sortCol: String, sortCols: String*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sort(sortCol:String,sortCols:String*):org.apache.spark.sql.Dataset[T]>`_
- `sortWithinPartitions(sortExprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sortWithinPartitions(sortExprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `sortWithinPartitions(sortCol: String, sortCols: String*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sortWithinPartitions(sortCol:String,sortCols:String*):org.apache.spark.sql.Dataset[T]>`_
- `transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#transform[U](t:org.apache.spark.sql.Dataset[T]=%3Eorg.apache.spark.sql.Dataset[U]):org.apache.spark.sql.Dataset[U]>`_
- `union(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#union(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `unionAll(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#unionAll(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `unionByName(other: Dataset[T], allowMissingColumns: Boolean): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#unionByName(other:org.apache.spark.sql.Dataset[T],allowMissingColumns:Boolean):org.apache.spark.sql.Dataset[T]>`_
- `unionByName(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#unionByName(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `where(conditionExpr: String): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#where(conditionExpr:String):org.apache.spark.sql.Dataset[T]>`_
- `where(condition: Column): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#where(condition:org.apache.spark.sql.Column):org.apache.spark.sql.Dataset[T]>`_

Untyped transformations
-----------------------
- `agg(expr: Column, exprs: Column*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#agg(expr:org.apache.spark.sql.Column,exprs:org.apache.spark.sql.Column*):org.apache.spark.sql.DataFrame>`_
- `agg(exprs: Map[String, String]): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#agg(exprs:Map[String,String]):org.apache.spark.sql.DataFrame>`_
- `agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#agg(aggExpr:(String,String),aggExprs:(String,String)*):org.apache.spark.sql.DataFrame>`_
- `apply(colName: String): Column <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#apply(colName:String):org.apache.spark.sql.Column>`_
- `col(colName: String): Column <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#col(colName:String):org.apache.spark.sql.Column>`_
- `colRegex(colName: String): Column <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#colRegex(colName:String):org.apache.spark.sql.Column>`_
- `drop(col: Column): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#drop(col:org.apache.spark.sql.Column):org.apache.spark.sql.DataFrame>`_
- `drop(colNames: String*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#drop(colNames:String*):org.apache.spark.sql.DataFrame>`_
- `drop(colName: String): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#drop(colName:String):org.apache.spark.sql.DataFrame>`_
- `groupBy(col1: String, cols: String*): RelationalGroupedDataset <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#groupBy(col1:String,cols:String*):org.apache.spark.sql.RelationalGroupedDataset>`_
- `groupBy(cols: Column*): RelationalGroupedDataset <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#groupBy(cols:org.apache.spark.sql.Column*):org.apache.spark.sql.RelationalGroupedDataset>`_
- `hashCode(): Int <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#hashCode():Int>`_
- `join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#join(right:org.apache.spark.sql.Dataset[_],joinExprs:org.apache.spark.sql.Column,joinType:String):org.apache.spark.sql.DataFrame>`_
- `join(right: Dataset[_], joinExprs: Column): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#join(right:org.apache.spark.sql.Dataset[_],joinExprs:org.apache.spark.sql.Column):org.apache.spark.sql.DataFrame>`_
- `join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#join(right:org.apache.spark.sql.Dataset[_],usingColumns:Seq[String],joinType:String):org.apache.spark.sql.DataFrame>`_
- `join(right: Dataset[_], usingColumns: Seq[String]): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#join(right:org.apache.spark.sql.Dataset[_],usingColumns:Seq[String]):org.apache.spark.sql.DataFrame>`_
- `join(right: Dataset[_], usingColumn: String): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#join(right:org.apache.spark.sql.Dataset[_],usingColumn:String):org.apache.spark.sql.DataFrame>`_
- `join(right: Dataset[_]): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#join(right:org.apache.spark.sql.Dataset[_]):org.apache.spark.sql.DataFrame>`_
- `na: DataFrameNaFunctions <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#na:org.apache.spark.sql.DataFrameNaFunctions>`_
- `select(col: String, cols: String*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select(col:String,cols:String*):org.apache.spark.sql.DataFrame>`_
- `select(cols: Column*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#select(cols:org.apache.spark.sql.Column*):org.apache.spark.sql.DataFrame>`_
- `selectExpr(exprs: String*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#selectExpr(exprs:String*):org.apache.spark.sql.DataFrame>`_
- `stat: DataFrameStatFunctions <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#stat:org.apache.spark.sql.DataFrameStatFunctions>`_
- `withColumn(colName: String, col: Column): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#withColumn(colName:String,col:org.apache.spark.sql.Column):org.apache.spark.sql.DataFrame>`_
- `withColumnRenamed(existingName: String, newName: String): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#withColumnRenamed(existingName:String,newName:String):org.apache.spark.sql.DataFrame>`_

Ungrouped
---------
- `encoder: Encoder[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#encoder:org.apache.spark.sql.Encoder[T]>`_
- `queryExecution: execution.QueryExecution <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#queryExecution:org.apache.spark.sql.execution.QueryExecution>`_
- `sameSemantics(other: Dataset[T]): Boolean <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sameSemantics(other:org.apache.spark.sql.Dataset[T]):Boolean>`_
- `semanticHash(): Int <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#semanticHash():Int>`_
- `sparkSession: SparkSession <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sparkSession:org.apache.spark.sql.SparkSession>`_
- `sqlContext: SQLContext <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sqlContext:org.apache.spark.sql.SQLContext>`_
- `toJSON: Dataset[String] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#toJSON:org.apache.spark.sql.Dataset[String]>`_
- `toString(): String <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#toString():String>`_

Unsupported operations
**********************

Actions
-------
- `describe(cols: String*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#describe(cols:String*):org.apache.spark.sql.DataFrame>`_
- `reduce(func: ReduceFunction[T]): T <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#reduce(func:org.apache.spark.api.java.function.ReduceFunction[T]):T>`_
- `reduce(func: (T, T) => T): T <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#reduce(func:(T,T)=%3ET):T>`_
- `summary(statistics: String*): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#summary(statistics:String*):org.apache.spark.sql.DataFrame>`_
- `tail(n: Int): Array[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#tail(n:Int):Array[T]>`_

Basic Dataset Functions
-----------------------
- `as[U](implicitevidence: Encoder[U]): Dataset[U] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#as[U](implicitevidence:org.apache.spark.sql.Encoder[U]):org.apache.spark.sql.Dataset[U]>`_
- `checkpoint(eager: Boolean): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#checkpoint(eager:Boolean):org.apache.spark.sql.Dataset[T]>`_
- `checkpoint(): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#checkpoint():org.apache.spark.sql.Dataset[T]>`_
- `persist(newLevel: org.apache.spark.storage.StorageLevel): Dataset.this.type <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#persist(newLevel:org.apache.spark.storage.StorageLevel):Dataset.this.type>`_
- `persist(): Dataset.this.type <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#persist():Dataset.this.type>`_

Typed transformations
---------------------
- `except(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#except(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `exceptAll(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#exceptAll(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `intersect(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#intersect(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `intersectAll(other: Dataset[T]): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#intersectAll(other:org.apache.spark.sql.Dataset[T]):org.apache.spark.sql.Dataset[T]>`_
- `observe(name: String, expr: Column, exprs: Column*): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#observe(name:String,expr:org.apache.spark.sql.Column,exprs:org.apache.spark.sql.Column*):org.apache.spark.sql.Dataset[T]>`_
- `sample(withReplacement: Boolean, fraction: Double): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sample(withReplacement:Boolean,fraction:Double):org.apache.spark.sql.Dataset[T]>`_
- `sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sample(withReplacement:Boolean,fraction:Double,seed:Long):org.apache.spark.sql.Dataset[T]>`_
- `sample(fraction: Double): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sample(fraction:Double):org.apache.spark.sql.Dataset[T]>`_
- `sample(fraction: Double, seed: Long): Dataset[T] <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#sample(fraction:Double,seed:Long):org.apache.spark.sql.Dataset[T]>`_

Untyped transformations
-----------------------
- `crossJoin(right: Dataset[_]): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#crossJoin(right:org.apache.spark.sql.Dataset[_]):org.apache.spark.sql.DataFrame>`_
- `cube(col1: String, cols: String*): RelationalGroupedDataset <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#cube(col1:String,cols:String*):org.apache.spark.sql.RelationalGroupedDataset>`_
- `cube(cols: Column*): RelationalGroupedDataset <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#cube(cols:org.apache.spark.sql.Column*):org.apache.spark.sql.RelationalGroupedDataset>`_
- `rollup(col1: String, cols: String*): RelationalGroupedDataset <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#rollup(col1:String,cols:String*):org.apache.spark.sql.RelationalGroupedDataset>`_
- `rollup(cols: Column*): RelationalGroupedDataset <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#rollup(cols:org.apache.spark.sql.Column*):org.apache.spark.sql.RelationalGroupedDataset>`_
- `explode[A, B](inputColumn: String, outputColumn: String)(f: A => TraversableOnce[B])(implicitevidence: reflect.runtime.universe.TypeTag[B]): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#explode[A,B](inputColumn:String,outputColumn:String)(f:A=%3ETraversableOnce[B])(implicitevidence$5:reflect.runtime.universe.TypeTag[B]):org.apache.spark.sql.DataFrame>`_
- `explode[A <: Product](input: Column*)(f: Row => TraversableOnce[A])(implicitevidence: reflect.runtime.universe.TypeTag[A]): DataFrame <https://spark.apache.org/docs/3.1.1/api/scala/org/apache/spark/sql/Dataset.html#explode[A%3C:Product](input:org.apache.spark.sql.Column*)(f:org.apache.spark.sql.Row=%3ETraversableOnce[A])(implicitevidence$4:reflect.runtime.universe.TypeTag[A]):org.apache.spark.sql.DataFrame>`_

`*` Cross joins and full outer joins are not supported. Aggregations with more than one distinct aggregate expression are not supported.

.. _udfs:

User-Defined Functions (UDFs)
#############################

To run a Spark SQL UDF within Opaque enclaves, first name it explicitly and define it in Scala, then reimplement it in C++ against Opaque's serialized row representation.

For example, suppose we wish to implement a UDF called ``dot``, which computes the dot product of two double arrays (``Array[Double]``). We [define it in Scala](src/main/scala/edu/berkeley/cs/rise/opaque/expressions/DotProduct.scala) in terms of the Breeze linear algebra library's implementation. We can then use it in a DataFrame query, such as `logistic regression <src/main/scala/edu/berkeley/cs/rise/opaque/benchmark/LogisticRegression.scala>`_.

Now we can port this UDF to Opaque as follows:

1. Define a corresponding expression using Opaque's expression serialization format by adding the following to [Expr.fbs](src/flatbuffers/Expr.fbs), which indicates that a DotProduct expression takes two inputs (the two double arrays):

   .. code-block:: protobuf
                   
                   table DotProduct {
                     left:Expr;
                     right:Expr;
                   }

   In the same file, add ``DotProduct`` to the list of expressions in ``ExprUnion``.

2. Implement the serialization logic from the Scala ``DotProduct`` UDF to the Opaque expression that we just defined. In ``Utils.flatbuffersSerializeExpression`` (from ``Utils.scala``), add a case for ``DotProduct`` as follows:

   .. code-block:: scala
                   
                   case (DotProduct(left, right), Seq(leftOffset, rightOffset)) =>
                     tuix.Expr.createExpr(
                       builder,
                       tuix.ExprUnion.DotProduct,
                       tuix.DotProduct.createDotProduct(
                         builder, leftOffset, rightOffset))


3. Finally, implement the UDF in C++. In ``FlatbuffersExpressionEvaluator#eval_helper`` (from ``expression_evaluation.h``), add a case for ``tuix::ExprUnion_DotProduct``. Within that case, cast the expression to a ``tuix::DotProduct``, recursively evaluate the left and right children, perform the dot product computation on them, and construct a ``DoubleField`` containing the result.

   
