// TODO: folder that contains 3 encrypted files similar to below:

// # word	count
// hello	1
// world	2

// Starting and stopping enclave. To support fault tolerance and speculative execution, will need to
// ensure that an enclave is running within the mapPartitions call of every operator
sqlContext.parallelize().foreachPartition { p =>
  val enclaveId = startEnclave()
  // store enclaveId in thread-local storage
}

// words needs schema: infer it?

val words = sqlContext.read.csv("data.enc", List('word.string, 'count.int)) // [word: string, count: int]

// requires a new data source: extend https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala
// based on https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVRelation.scala#L52
words.show


val counts = words.encFilter(encCol("count") > encLit(3)).encSelect(encCol("word"))
// emit special opcode for "x > 3"
// words.groupBy($"word").agg(sum("count"))
// Overview of query optimization: https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala
// Simple version of Project without codegen and tungsten: https://github.com/apache/spark/blob/branch-1.2/sql/core/src/main/scala/org/apache/spark/sql/execution/basicOperators.scala

// Problem: iterator model will leak information about which rows passed a predicate
// Solutions:
// (1) optimizer will insert random shuffle before every Filter
///    Advantage: better performance
//     Problem: this can sometimes leak the selectivity of the filter even if it's an intermediate result
//     words -> permute -> filter -> project
// (2) filter with tombstoning
//     Advantage: avoids leaking intermediate selectivity
//     Problem: sometimes have to iterate over more elements than in (1) because they contain tombstones
//     words -> filter w/tombstone -> project(word, tombstone) -> permute -> filter+project on tombstone

// filter -> broadcast join
//            ^
//     other -|

counts.show
// override Dataset.showString

// TODO:
enclaveRegisterPredicate: (Expression) => Int
enclaveEvalPredicate: (Int, Row) => Boolean
