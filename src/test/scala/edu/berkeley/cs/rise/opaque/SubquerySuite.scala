/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque

import org.apache.spark.sql.SparkSession

trait SubquerySuite extends OpaqueSQLSuiteBase with SQLHelper {
  import spark.implicits._

  val row = identity[(java.lang.Integer, java.lang.Double)](_)

  def numPartitions: Int

  override def queries = Seq(
    "SELECT c1, (select max(c1) from t2 where t1.c2 = t2.c2) from t1",
    "select a, sum(b) as s from l group by a having a > (select avg(a) from l)",
    "select * from l where exists (select * from r where l.a = r.c) and l.a <= 2",
    "select * from l where l.a in (select c from r) and l.a > 2 and l.b is not null",
    "select * from l where a not in (select c from r)",
    "select * from l where (a, b) not in (select c, d from t) and a < 4",
    "select * from l where (a, b) not in (select c, d from r where c > 10)",
    "select * from l where (a, b) not in (select c, d from r)",
    "select * from l where (a, b) not in (select c, d from t) and (a + b) is not null",
    "select a from l l1 where a in (select a from l where a < 3 group by a)",
    "select l.a from l where (select count(*) + sum(r.d) from r where l.a = r.c) = 0",
    """
      | select c1 from t1
      | where c2 IN (select c2 from t2)
      |
    """.stripMargin,
    """
        | select c1 from t1
        | where c2 NOT IN (select c2 from t2)
        |
    """.stripMargin,
    """
      | select c1 from t1
      | where EXISTS (select c2 from t2)
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where NOT EXISTS (select c2 from t2)
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where NOT EXISTS (select c2 from t2) and
      |       c2 IN (select c2 from t3)
      |
      """.stripMargin,
    """
      |select a
      |from   l
      |where  (select count(*)
      |        from   r
      |        where (a = c and d = 2.0) or (a = c and d = 1.0)) > 0
    """.stripMargin,
    "select l.a from l where (select count(*) from r where l.a = r.c) < l.a",
    """
      |select l.b, (select (r.c + count(*)) is null
      |from r
      |where l.a = r.c group by r.c) from l
    """.stripMargin,
    """
      | select c1 from onerow t1
      | where exists (select 1 from onerow t2 where t1.c1=t2.c1)
      | and   exists (select 1 from onerow LIMIT 1)
    """.stripMargin,
    """
      | select c1 from onerow t1
      | where exists (select 1
      |               from   (select c1 from onerow t2 LIMIT 1) t2
      |               where  t1.c1=t2.c1)
    """.stripMargin,
    """
      | select *
      | from   (select t2.c2+1 as c3
      |         from   t1 left join t2 on t1.c1=t2.c2) t3
      | where  c3 not in (select c2 from t2)
    """.stripMargin,
    """
      |SELECT c1 FROM t1
      |WHERE
      |c1 IN (SELECT c1 FROM t2 ORDER BY c1)
    """.stripMargin,
    """
      | select c1
      | from   t1
      | where  c1 in (select t2.c1
      |               from   t2
      |               where  t1.c2 >= t2.c2)
    """.stripMargin,
    """
      |SELECT c1
      |FROM   t1
      |WHERE  c1 IN (SELECT c1
      |              FROM   (SELECT *
      |                      FROM   t2
      |                      ORDER  BY c2)
      |              ORDER  BY c1)
    """.stripMargin,
    """
      |SELECT c1
      |FROM   t1
      |WHERE  c1 IN (SELECT c1
      |              FROM   (SELECT c1, c2, count(*)
      |                      FROM   t2
      |                      GROUP BY c1, c2
      |                      HAVING count(*) > 0
      |                      ORDER BY c2)
      |              ORDER  BY c1)
    """.stripMargin,
    """
      |SELECT c1
      |FROM   t1
      |WHERE  c1 IN (SELECT c1
      |              FROM   t2
      |              WHERE  c1 IN (SELECT c1
      |                            FROM   t3
      |                            WHERE  c1 = 1
      |                            ORDER  BY c3)
      |              ORDER  BY c2)
    """.stripMargin
  )
  override def failingQueries = Seq(
    "select (select key from subqueryData where key > 2 order by key limit 1) + 1",
    "select -(select max(key) from subqueryData)",
    "select (select min(value) from subqueryData" +
      " where key = (select max(key) from subqueryData) - 1)",
    "SELECT (select 1 as col) from t1",
    "SELECT (select max(c1) from t2) from t1",
    "SELECT 1 + (select 1 as col) from t1",
    "select * from l where exists (select * from r where l.a = r.c)",
    "select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)",
    "select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)" +
      " or not exists (select * from r where l.a = r.c)",
    "select * from l where l.a in (select c from r)",
    "select * from l where l.a in (select c from r where l.b < r.d)",
    "select * from l where a not in (select c from r where c is not null)",
    "select * from l where l.a in (select c from r)" +
      " or l.a in (select c from r where l.b < r.d)",
    "select * from l where a not in (select c from r)" +
      " or a not in (select c from r where c is not null)",
    "select a from l group by 1 having exists (select 1 from r where d < min(b))",
    "select * from l where b < (select max(d) from r where a = c)",
    "select a, (select sum(b) from l l2 where l2.a = l1.a) sum_b from l l1",
    "select a, (select sum(d) from r where a = c) sum_d from l l1 group by 1, 2",
    "select l.a from l where (select count(*) from r where l.a = r.c) = 0",
    "select l.a from l where (select sum(r.d) is null from r where l.a = r.c)",
    "select a, (select count(*) from r where l.a = r.c) as cnt from l",
    "select l.a as grp_a from l group by l.a " +
      "having (select count(*) from r where grp_a = r.c) = 0 " +
      "order by grp_a",
    "select l.a as aval, sum((select count(*) from r where l.a = r.c)) as cnt " +
      "from l group by l.a order by aval",
    "select l.a from l where (select sum(r.d) from r where l.a = r.c) is null",
    "select l.a from l where (select count(*) from r where l.a = r.c) > 0",
    """select l.a from l
      |where (
      |    select cntPlusOne + 1 as cntPlusTwo from (
      |        select cnt + 1 as cntPlusOne from (
      |            select sum(r.c) s, count(*) cnt from r where l.a = r.c having cnt = 0
      |        )
      |    )
      |) = 2
    """.stripMargin,
    "SELECT c1, (select max(c1) from t2) + c2 from t1",
    "select l.a from l where " +
      "(select case when count(*) = 1 then null else count(*) end as cnt " +
      "from r where l.a = r.c) = 0",
    "select * from l, r where l.a = r.c + 1 AND (exists (select * from r) OR l.a = r.c)",
    "select * from t1 where id in (select id as id from t1)",
    "select * from t1 where id in (select id as id from t2)",
    """
      |SELECT * FROM t1 a
      |WHERE
      |NOT EXISTS (SELECT * FROM t1 b WHERE a.i = b.i)
    """.stripMargin,
    """
      |SELECT * FROM l, r WHERE l.a = r.c + 1 AND
      |(EXISTS (SELECT * FROM r) OR l.a = r.c)
    """.stripMargin
  )
  override def unsupportedQueries = Seq(
    "select (select 1 as b) as b",
    "select (select (select 1) + 1) + 1",
    "select (select 's' as s) as b",
    "select * from range(10) where id not in (select id from range(2) union all select id from range(2))",
    """
      | with t2 as (with t1 as (select 1 as b, 2 as c) select b, c from t1)
      | select a from (select 1 as a union all select 2 as a) t
      | where a = (select max(b) from t2)
    """.stripMargin,
    """
      | with t2 as (with t1 as (select 1 as b, 2 as c) select b, c from t1),
      | t3 as (
      |   with t4 as (select 1 as d, 3 as e)
      |   select * from t4 cross join t2 where t2.b = t4.d
      | )
      | select a from (select 1 as a union all select 2 as a)
      | where a = (select max(d) from t3)
    """.stripMargin,
    "with t2 as (select 1 as b, 2 as c) " +
      "select a from (select 1 as a union all select 2 as a) t " +
      "where a = (select max(b) from t2)",
    "select (select 's' as s limit 0) as b",
    "select (select value from subqueryData limit 0)",
    """
      | select c1 from t1
      | where (case when c2 IN (select 1 as one) then 1
      |       else 2 end) = c1
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where (case when c2 IN (select 1 as one) then 1
            |       else 2 end)
      |       IN (select c2 from t2)
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where (case when c2 IN (select c2 from t2) then 1
      |             else 2 end)
      |       IN (select c2 from t3)
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where (case when c2 IN (select c2 from t2) then 1
      |             when c2 IN (select c2 from t3) then 2
      |             else 3 end)
      |       IN (select c2 from t1)
      |
    """.stripMargin,
    """
        | select c1 from t1
        | where (c1, (case when c2 IN (select c2 from t2) then 1
        |                  when c2 IN (select c2 from t3) then 2
        |                  else 3 end))
        |       IN (select c1, c2 from t1)
        |
    """.stripMargin,
    """
      | select c1 from t3
      | where ((case when c2 IN (select c2 from t2) then 1 else 2 end),
      |        (case when c2 IN (select c2 from t3) then 2 else 3 end))
      |     IN (select c1, c2 from t3)
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where ((case when EXISTS (select c2 from t2) then 1 else 2 end),
      |        (case when c2 IN (select c2 from t3) then 2 else 3 end))
      |     IN (select c1, c2 from t3)
      |
    """.stripMargin,
    """
      | select c1 from t1
      | where (case when c2 IN (select c2 from t2) then 3
      |             else 2 end)
      |       NOT IN (select c2 from t3)
      |
      """.stripMargin,
    """
      | select c1 from t1
      | where ((case when c2 IN (select c2 from t2) then 1 else 2 end),
      |        (case when NOT EXISTS (select c2 from t3) then 2
      |              when EXISTS (select c2 from t2) then 3
      |              else 3 end))
      |     NOT IN (select c1, c2 from t3)
      |
    """.stripMargin,
    "select a, (select sum(b) from l l2 where l2.a <=> l1.a) sum_b from l l1",
    """
      | select t1.c1
      | from   t1, t1 as t3
      | where  t1.c1 = t3.c1
      | and    (t1.c1 in (select t2.c1
      |                   from   t2
      |                   where  t1.c2 >= t2.c2
      |                          or t3.c2 < t2.c2)
      |         or t1.c2 >= 0)
    """.stripMargin,
    """
      | SELECT c2
      | FROM t1
      | WHERE EXISTS (SELECT *
      |               FROM t2 LATERAL VIEW explode(arr_c2) q AS c2
      |                   WHERE t1.c1 = t2.c1)
    """.stripMargin,
    "select * from l, r where l.a = r.c AND (r.d in (select d from r) OR l.a >= 1)"
  )

  override def loadTestData(sqlStr: String, sl: SecurityLevel) = {
    super.loadTestData(sqlStr, sl)
    loadSubqueryData(sl)
  }

  def loadSubqueryData(sl: SecurityLevel) = {
    lazy val l = sl.applyTo(
      Seq(
        row((1, 2.0)),
        row((1, 2.0)),
        row((2, 1.0)),
        row((2, 1.0)),
        row((3, 3.0)),
        row((null, null)),
        row((null, 5.0)),
        row((6, null))
      ).toDF("a", "b")
    )
    lazy val r = sl.applyTo(
      Seq(
        row((2, 3.0)),
        row((2, 3.0)),
        row((3, 2.0)),
        row((4, 1.0)),
        row((null, null)),
        row((null, 5.0)),
        row((6, null))
      ).toDF("c", "d")
    )
    lazy val t = sl.applyTo(r.filter($"c".isNotNull && $"d".isNotNull))
    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
    t.createOrReplaceTempView("t")

    val subqueryData = sl.applyTo(Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value"))
    subqueryData.createOrReplaceTempView("subqueryData")

    sl.applyTo(Seq((1, 1), (2, 2)).toDF("c1", "c2")).createOrReplaceTempView("t1")
    sl.applyTo(Seq((1, 1), (2, 2)).toDF("c1", "c2")).createOrReplaceTempView("t2")
    sl.applyTo(Seq((1, 1, 1), (2, 2, 2), (1, 2, 3)).toDF("c1", "c2", "c3"))
      .createOrReplaceTempView("t3")
    sl.applyTo(Seq(1).toDF("c1")).createOrReplaceTempView("onerow")
  }
}

class SinglePartitionSubquerySuite extends SubquerySuite {
  override def numPartitions = 1
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SinglePartitionSubquerySuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries()
}

class MultiplePartitionSubquerySuite extends SubquerySuite {
  val executorInstances = 3

  override def numPartitions = executorInstances
  override val spark = SparkSession
    .builder()
    .master(s"local-cluster[$executorInstances,1,1024]")
    .appName("MultiplePartitionSubquerySuite")
    .config("spark.executor.instances", executorInstances)
    .config("spark.sql.shuffle.partitions", numPartitions)
    .config(
      "spark.jars",
      "target/scala-2.12/opaque_2.12-0.1.jar,target/scala-2.12/opaque_2.12-0.1-tests.jar"
    )
    .getOrCreate()

  runSQLQueries()
}
