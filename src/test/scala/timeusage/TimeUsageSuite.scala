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

    //summaryDf.orderBy($"primaryNeeds".desc, $"work".desc, $"other".desc).show()

    val u = summaryDf.orderBy($"primaryNeeds".desc, $"work".desc, $"other".desc).first()

    assert(u(0) === "working")
    assert(u(1) === "female")
    assert(u(2) === "active")
    assert(u(3) === 24.0)
    assert(u(4) === 0.0)
    assert(u(5) === 0.0)
  }

  test("timeUsageGrouped") {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)


    //finalDf.orderBy($"primaryNeeds".desc, $"work".desc, $"other".desc).show()
    val u = finalDf.orderBy($"primaryNeeds".desc, $"work".desc, $"other".desc).first()

    assert(u(0) === "not working")
    assert(u(1) === "female")
    assert(u(2) === "young")
    assert(u(3) === 12.5)
    assert(u(4) === 0.2)
    assert(u(5) === 11.1)
  }
}
