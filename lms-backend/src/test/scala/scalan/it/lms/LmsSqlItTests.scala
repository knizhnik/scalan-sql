package scalan
package it.lms

import scalan.community.ScalanCommunityDslExp
import scalan.compilation.lms._
import scalan.compilation.lms.scalac.LmsCompilerScala
import scalan.it.smoke.CommunitySqlItTests
import scalan.sql._

class LmsSqlItTests extends CommunitySqlItTests {
  class ProgExp extends ProgCommunity with ScalanCommunityDslExp with SqlDslExp with TablesDslExp with LmsCompilerScala { self =>
    def makeBridge[A, B] = new CoreBridge[A, B] {
      val scalan = self
      val lms = new CommunityLmsBackend
    }
  }
  
  override val progStaged = new ProgExp

  test("test24simpleSelectTest") {
    val in = Array((1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (5, 5.5))
    compareOutputWithSequential(progStaged)(progSeq.selectUsingIndex, progStaged.selectUsingIndex, "selectUsingIndex", in)
  }

  test("test25innerJoin") {
    val in = (Array((1, 1.0), (2, 0.0), (3, 1.0), (4, 1.0), (5, 0.0)), Array(("one", 1), ("two", 2), ("three", 3), ("four", 4), ("five", 5)))
    compareOutputWithSequential(progStaged)(progSeq.innerJoin, progStaged.innerJoin, "innerJoin", in)
  }

  test("test26hashJoin") {
    val in = (Array((1, 1.0), (2, 0.0), (3, 1.0), (4, 1.0), (5, 0.0)), Array(("one", 1), ("two", 2), ("three", 1), ("four", 4), ("five", 5)))
    compareOutputWithSequential(progStaged)(progSeq.hashJoin, progStaged.hashJoin, "hashJoin", in)
  }
  test("test27simpleIf") {
    val in = (Array(2.0,3.0), 4.0)
    compareOutputWithSequential(progStaged)(progSeq.simpleIf, progStaged.simpleIf, "simpleIf", in)
  }
  /*
  test("test27ifTest") {
    val in = (1, 0.0)
    compareOutputWithSequential(progStaged)(progSeq.ifTest, progStaged.ifTest, "ifTest", in)
  }
  */
  test("test28selectCount") {
    val in = Array((1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (5, 5.5))
    compareOutputWithSequential(progStaged)(progSeq.selectCount, progStaged.selectCount, "selectCount", in)
  }
  test("test29sqlBenchmark") {
    val in = Array((1, "11243"), (2, "21235"), (3, "12343"), (4, "13455"), (5, "543123"))
    compareOutputWithSequential(progStaged)(progSeq.sqlBenchmark, progStaged.sqlBenchmark, "sqlBenchmark", in)
  }
  test("test30groupBy") {
    val in = Array((1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (5, 5.5))
    compareOutputWithSequential(progStaged)(progSeq.groupBy, progStaged.groupBy, "groupBy", in)
  }
  test("test31columnarStore") {
    val in = Array((1, "11243"), (2, "21235"), (3, "12343"), (4, "13455"), (5, "543123"))
    compareOutputWithSequential(progStaged)(progSeq.columnarStore, progStaged.columnarStore, "columnarStore", in)
  }
  test("testColumnarStoreR3") {
    val in = Array(("11243", (1, 1.0)), ("21235", (2, 2.0)), ("12343", (3, 3.0)), ("13455", (4, 4.0)), ("543123", (5, 5.0)))
    compareOutputWithSequential(progStaged)(progSeq.columnarStoreR3, progStaged.columnarStoreR3, "columnarStoreR3", in)
  }
  test("test32sqlParBenchmark") {
    val in = Array((1, "11243"), (2, "21235"), (3, "12343"), (4, "13455"), (5, "543123"))
    compareOutputWithSequential(progStaged)(progSeq.sqlParBenchmark, progStaged.sqlParBenchmark, "sqlParBenchmark", in)
  }
  test("test34sqlIndexBenchmark") {
    val in = Array((1, "11243"), (2, "21235"), (3, "12343"), (4, "13455"), (5, "543123"))
    compareOutputWithSequential(progStaged)(progSeq.sqlIndexBenchmark, progStaged.sqlIndexBenchmark, "sqlIndexBenchmark", in)
  }
  test("test35sqlGroupBy") {
    val in = Array((1, "red"), (2, "green"), (3, "red"), (4, "blue"), (5, "blue"), (6, "green"), (7, "red"), (8, "blue"), (9, "red"))
    compareOutputWithSequential(progStaged)(progSeq.sqlGroupBy, progStaged.sqlGroupBy, "sqlGroupBy", in)
  }
  test("test36sqlMapReduce") {
    val in = (4, Array((1, "red"), (2, "green"), (3, "red"), (4, "blue"), (5, "blue"), (6, "green"), (7, "red"), (8, "blue"), (9, "red")))
    compareOutputWithSequential(progStaged)(progSeq.sqlMapReduce, progStaged.sqlMapReduce, "sqlMapReduce", in)
  }
  test("test37sqlParallelJoin") {
    val in = (Array((1, "red"), (2, "green"), (3, "blue")), (Array((1, "one"), (2, "two"), (3, "three")), Array((1, "true"), (2, "false"), (3, "unknown"))))
    compareOutputWithSequential(progStaged)(progSeq.sqlParallelJoin, progStaged.sqlParallelJoin, "sqlParallelJoin", in)
  }
  test("test38sqlAggJoin") {
    val in = (Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)),
      (Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)),
        (Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)),
          (Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)),
            (Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)), 4)))))
    compareOutputWithSequential(progStaged)(progSeq.sqlAggJoin, progStaged.sqlAggJoin, "sqlAggJoin", in)
  }
  test("test39sqlIndexJoin") {
    val in = (Array((1, "red"), (2, "green"), (3, "blue")), Array((1, "one"), (2, "two"), (1, "three"), (1, "four"), (2, "five")))
    compareOutputWithSequential(progStaged)(progSeq.sqlIndexJoin, progStaged.sqlIndexJoin, "sqlIndexJoin", in)
  }
  test("test40sqlColumnarStoreBenchmark") {
    val in = (4, Array((1, "11243"), (2, "21235"), (3, "12343"), (4, "13455"), (5, "543123")))
    compareOutputWithSequential(progStaged)(progSeq.sqlColumnarStoreBenchmark, progStaged.sqlColumnarStoreBenchmark, "sqlColumnarStoreBenchmark", in)
  }
  test("test41sqlDsl") {
    val in = 10
    compareOutputWithSequential(progStaged)(progSeq.sqlDsl, progStaged.sqlDsl, "sqlDsl", in)
  }

  val tpch_data = "benchmarks/sql/lineitem-small.tbl"
  
  test("tpchQ1") {
    val in = scala.io.Source.fromFile(tpch_data).getLines().map(s => s.split("\\|")).toArray
    compareOutputWithSequential(progStaged)(progSeq.tpchQ1, progStaged.tpchQ1, "tpchQ1", in)
  }

  test("TPCH_Q1_hor_seq") {
    val in = scala.io.Source.fromFile(tpch_data).getLines().map(s => s.split("\\|")).toArray
    compareOutputWithSequential(progStaged)(progSeq.TPCH_Q1_hor_seq, progStaged.TPCH_Q1_hor_seq, "TPCH_Q1_hor_seq", in)
  }

  test("TPCH_Q1_ver_seq") {
    val in = scala.io.Source.fromFile(tpch_data).getLines().map(s => s.split("\\|")).toArray
    compareOutputWithSequential(progStaged)(progSeq.TPCH_Q1_ver_seq, progStaged.TPCH_Q1_ver_seq, "TPCH_Q1_ver_seq", in)
  }

  test("TPCH_Q1_hor_par") {
    val in = scala.io.Source.fromFile(tpch_data).getLines().map(s => s.split("\\|")).toArray
    compareOutputWithSequential(progStaged)(progSeq.TPCH_Q1_hor_par, progStaged.TPCH_Q1_hor_par, "TPCH_Q1_hor_par", in)
  }

  test("TPCH_Q1_ver_par") {
    val in = scala.io.Source.fromFile(tpch_data).getLines().map(s => s.split("\\|")).toArray
    compareOutputWithSequential(progStaged)(progSeq.TPCH_Q1_ver_par, progStaged.TPCH_Q1_ver_par, "TPCH_Q1_ver_par", in)
  }

  test("test42Tuple") {
    val in = (Array((1, "red"), (2, "green"), (3, "blue")), Array((1, "one"), (2, "two"), (3, "three")))
    compareOutputWithSequential(progStaged)(progSeq.testTuple, progStaged.testTuple, "testTuple", in)
  }
}
