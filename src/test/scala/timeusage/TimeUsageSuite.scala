package timeusage

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage._
import spark.implicits._

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    spark.stop()
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

    val u = summaryDf.first()

    assert(u(0) === "working")
    assert(u(1) === "male")
    assert(u(2) === "elder")
    assert(u(3) === 15.25)
    assert(u(4) === 0.0)
    assert(u(5) === 8.75)
  }

  test("timeUsageGrouped") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)


    val u = finalDf.first()

    assert(u(0) === "not working")
    assert(u(1) === "female")
    assert(u(2) === "active")
    assert(u(3) === 12.4)
    assert(u(4) === 0.5)
    assert(u(5) === 10.8)
  }

  test("timeUsageGroupedSql") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGroupedSql(summaryDf)


    val u = finalDf.first()

    assert(u(0) === "not working")
    assert(u(1) === "female")
    assert(u(2) === "active")
    assert(u(3) === 12.4)
    assert(u(4) === 0.5)
    assert(u(5) === 10.8)
  }

  test("timeUsageSummaryTyped") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val summaryDs = timeUsageSummaryTyped(summaryDf)

    summaryDs.show()
    val u = summaryDs.first()

    assert(u.working === "working")
    assert(u.sex === "male")
    assert(u.age === "elder")
    assert(u.primaryNeeds === 15.25)
    assert(u.work === 0.0)
    assert(u.other === 8.75)
  }

  test("timeUsageGroupedTyped") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val summaryDs = timeUsageSummaryTyped(summaryDf)
    val finalDf = timeUsageGroupedTyped(summaryDs)

    val u = finalDf.first()

    assert(u.working === "not working")
    assert(u.sex === "female")
    assert(u.age === "active")
    assert(u.primaryNeeds === 12.4)
    assert(u.work === 0.5)
    assert(u.other === 10.8)
  }
}
