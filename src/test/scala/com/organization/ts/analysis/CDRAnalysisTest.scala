package com.organization.ts.analysis

import com.holdenkarau.spark.testing.SharedSparkContext
import com.organization.SparkSessionTestWrapper
import com.organization.ts.analysis.CDRAnalysis
import com.organization.ts.schema.AllCaseClasses
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class CDRAnalysisTest  extends FlatSpec with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  "peakHrFromDailyBasis" should "create Dataframe" in {

    val sourceDF = Seq(
      (10000, 1385852400000L, 39, 0.4102033306112959, 0.4655506047647025, 0.011675588702365298, 0.022239611295775192, 9.38136791018439),
      (10000, 1385853000000L, 39, 0.964562073920518,	0.5337889645456183,	0.04377696114720513,	0.05545254984957043,	9.969030230091432)
    ).toDF("squareId", "timeInterval","countryCode", "smsInActivity", "smsOutActivity", "callInActivity", "callOutActivity", "internetTrafficActivity")

    val actualDF = CDRAnalysis.peakHrFromDailyBasis(spark, sourceDF)

    val expectedSchema = List(
      StructField("squareId", LongType, true),
      StructField("date", StringType, true),
      StructField("peak_hour", IntegerType, true),
      StructField("activity", DoubleType, true)
    )

    val expectedData = Seq(
      (10000, "2013-12-01", 23, 21.85764782511287)
    ).toDF("squareId", "date", "peak_hour", "activity")

    println(actualDF.show(1, false))
    assert(actualDF.show(1, false) === expectedData.show(1, false))
    assert(actualDF.schema.size === expectedData.schema.size)
  }

  "hourlyUsagePerId" should "create activity per hour Dataframe" in {

    val sourceDF = Seq(
      (10000, 1385852400000L, 39, 0.4102033306112959, 0.4655506047647025, 0.011675588702365298, 0.022239611295775192, 9.38136791018439),
      (10001, 1385853000000L, 39, 0.964562073920518,	0.5337889645456183,	0.04377696114720513,	0.05545254984957043,	9.969030230091432)
    ).toDF("squareId", "timeInterval","countryCode", "smsInActivity", "smsOutActivity", "callInActivity", "callOutActivity", "internetTrafficActivity")

    val actualDF = CDRAnalysis.hourlyUsagePerId(spark, sourceDF)

    val expectedData = Seq(
      (10000, "2013-12-01", 23, 10.291037045558529),
      (10001, "2013-12-01", 23, 11.566610779554344)
    ).toDF("squareId", "Date", "hour", "activity_per_hr")

    println(actualDF.show(2, false))
    assert(actualDF.schema.size === expectedData.schema.size)
  }


}
