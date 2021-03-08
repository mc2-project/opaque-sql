package edu.berkeley.cs.rise.opaque

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

trait SortSuite extends OpaqueSuiteBase with SQLHelper {
  import spark.implicits._

  def numPartitions: Int

  test("sort") {
    checkAnswer() { sl =>
      val pairs = sl.applyTo(Seq((1, 0), (2, 0), (0, 0), (3, 0)).toDF)
      pairs.sort()
    }
  }

  test("large array") {
    val unencrypted = generateRandomPairs()
    checkAnswer() { sl =>
      val pairs = sl.applyTo(unencrypted)
      pairs.sort()
    }
  }

  test("sort descending") {
    val unencrypted = generateRandomPairs()
    checkAnswer() { sl =>
      val pairs = sl.applyTo(unencrypted)
      pairs.sort(desc(pairs.columns(0)))
    }
  }

  test("empty DataFrame") {
    val unencrypted = Seq[(Int, Int)]().toDF
    checkAnswer() { sl =>
      val pairs = sl.applyTo(unencrypted)
      pairs.sort()
    }
  }

  test("basic sorting") {
    def loadInput(sl: SecurityLevel) = {
      sl.applyTo(Seq(("Hello", 4, 2.0), ("Hello", 1, 1.0), ("World", 8, 3.0)).toDF("a", "b", "c"))
    }

    checkAnswer() { sl =>
      val input = loadInput(sl)
      input.sort('a, 'b)
    }

    checkAnswer() { sl =>
      val input = loadInput(sl)
      input.sort('b, 'a)
    }
  }

  test("sorting all nulls") {
    checkAnswer() { sl =>
      val input = sl.applyTo((1 to 100).map(v => Tuple1(v)).toDF.selectExpr("NULL as a"))
      input.sort()
    }
  }

  test("sort followed by limit") {
    checkAnswer() { sl =>
      val input = sl.applyTo((1 to 100).map(v => Tuple1(v)).toDF("a"))
      input.sort()
    }
  }

  test("sorting does not crash for large inputs") {
    val stringLength = 1024 * 1024 * 2
    checkAnswer() { sl =>
      val input = Seq(Tuple1("a" * stringLength), Tuple1("b" * stringLength)).toDF("a")
      input.sort()
    }
  }

  def generateRandomPairs() = {
    val rand = new scala.util.Random()
    Seq.fill(1000) { (rand.nextInt(), rand.nextInt()) }.toDF
  }
}

class MultiplePartitionSortSuite extends SortSuite {
  override def numPartitions = 3
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MultiplePartitionFilterSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()
}
