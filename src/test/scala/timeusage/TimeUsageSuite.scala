package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage.{classifiedColumns, read, timeUsageSummary}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TimeUsage.spark.stop()
  }

  test("TimeUsage can be instantiated") {
    val instantiatable = try {
      TimeUsage
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a TimeUsage object")
  }

  test("row") {
    val r = TimeUsage.row(List("string", "1", "2"))
    assert(r(0) === "string")
    assert(r(1) === 1)
    assert(r(2) === 2)
  }

  test("timeUsageSummary") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    //summaryDf.show()

    val u = summaryDf.first()

    assert(u(0) === "working")
    assert(u(1) === "male")
    assert(u(2) === "elder")
    assert((u.getDouble(3) - 15.25).abs < 0.1)
    assert((u.getDouble(4) - 0).abs < 0.1)
    assert((u.getDouble(5) - 8.75).abs < 0.1)
  }
}
