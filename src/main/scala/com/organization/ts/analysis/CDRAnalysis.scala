package com.organization.ts.analysis

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.organization.ts.schema.{AllCaseClasses, AllSchema}
import com.organization.ts.utils.ConversionsData.{convertOptions, is_anomalous, labelTheCountry}
import com.organization.ts.utils.DateUtils.{dateFromEpoch, epochToDate, extractHrFromEpoch}
import com.organization.ts.utils.RowHasher
import org.apache.spark.util.LongAccumulator

object CDRAnalysis {
  import org.apache.spark.sql.functions._

  /**
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, Input HDFS Path for Preprocessing data
    * @return Dataframe, Its cleaned data and it can used for various other analytics.
    */
  def dataPreprocessing(spark:SparkSession, inputPath: String) = {
    import spark.implicits._
    val cdr = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(AllSchema.schemaCDR)
      .load(inputPath).as[AllCaseClasses.CallDetailRecordsWithOption]

    // Total Input Rows count
    val totalInputCount: LongAccumulator = spark.sparkContext.longAccumulator("TotalRecords")

    // I feel Square Id is very sensitive, and hashing with MD5
    // Drop Duplicates by columns : squareId, timeInterval, countryCode
    val dataProcessing = cdr.map(t => (RowHasher.md5Hash(t.squareId), t.timeInterval, t.countryCode, convertOptions(t.smsOutActivity), convertOptions(t.smsOutActivity),convertOptions(t.callInActivity),convertOptions(t.callOutActivity),convertOptions(t.internetTrafficActivity)))
      .map(x => (x, totalInputCount.add(1))).map( t => t._1)
      .toDF("squareId", "timeInterval", "countryCode", "smsInActivity", "smsOutActivity", "callInActivity", "callOutActivity", "internetTrafficActivity")
      .dropDuplicates("squareId", "timeInterval", "countryCode")

    val totalOutputCount: Long = dataProcessing.count()

    (dataProcessing, totalInputCount.sum, totalOutputCount)
  }

  /**
    *  Task 1
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, HDFS input path to analyze Sqaure Id activity per day
    * @return Dataframe, Peak Hour from Daily basis activity of Square Id
    *
    *  Example: Input Data
    *  squareId     timeinterval    country   smsInActivity smsOutActivity callInactivity callOutActivity internetActivity
    *
    *  1001         1385853000000   39        1.2           0.1             2.2           1.4             0.0
    *  1002         1385853000000   38        1.2           0.1             2.2           1.4             3.0
    *  1001         1385853600000   39        1.2           0.1             2.2           1.4             5.2
    *  1002         1385853600000   38        1.2           0.1             2.2           1.4             0.0
    *
    *  And Result could be
    *
    *  squareId     date          peak_hour     activity
    *
    *  1001         2013-11-30      23            xx
    *  1002         2014-01-01      04            yy
    *
    *
    */
  def peakHrFromDailyBasis(spark: SparkSession, inputDF: DataFrame) = {
    import spark.implicits._
    import org.apache.spark.sql.expressions.scalalang._
    val cdr = inputDF.as[AllCaseClasses.CallDetailRecords]

    cdr.map(t => (t.squareId, t.timeInterval,
      t.countryCode, t.callInActivity+t.callOutActivity+t.smsInActivity+t.smsOutActivity+t.internetTrafficActivity,
      dateFromEpoch(t.timeInterval), extractHrFromEpoch(t.timeInterval))).rdd.map(r => ((r._1,r._5,r._6), r._4))
      .reduceByKey(_ + _)
      .map(t => ((t._1._1, t._1._2),(t._1._3, t._2)))
      .reduceByKey((r1, r2) => if(r1._2 > r2._2) r1 else r2)
      .map(t => (t._1._1,t._1._2, t._2._1, t._2._2))
      .sortBy(_._1).toDF("squareId", "Date", "peak_hour", "activity")
  }

  /**
    * Task 2
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, HDFS input path to analyze Sqaure Id activity per day
    * @return Dataframe, Hourly Total Activity Usage of Square Id
    *
    *  Example: Input Data
    *  squareId     timeinterval    country   smsInActivity smsOutActivity callInactivity callOutActivity internetActivity
    *
    *  1001         1385853000000   39        1.2           0.1             2.2           1.4             0.0
    *  1002         1385853000000   38        1.2           0.1             2.2           1.4             3.0
    *  1001         1385853600000   39        1.2           0.1             2.2           1.4             5.2
    *  1002         1385853600000   38        1.2           0.1             2.2           1.4             0.0
    *
    *  And Result could be
    *
    *  squareId     date          hour          activity_per_hr
    *
    *  1001         2013-11-30      23            xx
    *  1001         2013-11-30      22            x1
    *  1001         2013-11-30      21            x2
    *  1001         2013-11-30      20            x3
    *  1001         2013-11-30      12            x4
    *  1002         2014-01-01      04            yy
    *  1002         2014-01-01      05            y1
    *  1002         2014-01-01      06            y2
    *  1002         2014-01-01      07            y3
    *
    */
  def hourlyUsagePerId(spark: SparkSession, inputDF: DataFrame): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.expressions.scalalang._
    val cdr = inputDF.as[AllCaseClasses.CallDetailRecords]

    cdr.map(t => (t.squareId, t.timeInterval,
      t.countryCode, t.callInActivity+t.callOutActivity+t.smsInActivity+t.smsOutActivity+t.internetTrafficActivity,
      dateFromEpoch(t.timeInterval), extractHrFromEpoch(t.timeInterval))).rdd
      .map(r => ((r._1,r._5,r._6), r._4))
      .reduceByKey(_ + _)
      .map(r => (r._1._1, r._1._2, r._1._3, r._2))
      .sortBy(_._1).toDF("squareId", "Date", "hour", "activity_per_hr")
  }

  /**
    *  Task 3
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, HDFS input path to analyze Sqaure Id activity per day
    * @return Dataframe, Daily basis activity of Square Id
    *
    *  Example: Input Data
    *  squareId     timeinterval    country   smsInActivity smsOutActivity callInactivity callOutActivity internetActivity
    *
    *  1001         1385853000000   39        1.2           0.1             2.2           1.4             0.0
    *  1002         1385853000000   38        1.2           0.1             2.2           1.4             3.0
    *  1001         1385853600000   39        1.2           0.1             2.2           1.4             5.2
    *  1002         1385853600000   38        1.2           0.1             2.2           1.4             0.0
    *  ..
    *  ..
    *  ..
    *  .
    *
    *  And Result could be
    *
    *  squareId     date          activity_per_day(Rank in decreasing order i.e High values to low values)
    *
    *  1001         2013-11-30      121.1
    *  1002         2015-11-30      120.1
    *  1001         2014-11-30      119.1
    *  1004         2013-11-30      118.1
    *  1005         2013-11-30      117.1
    *  1009         2014-01-01      116.1
    *  1011         2014-01-11      115.1
    *  1012         2014-01-01      114.1
    *  ..
    *  ..
    *  ..
    *  .
    *
    */
  def squareIdRankPerDaily(spark: SparkSession, inputDF: DataFrame) = {
    import spark.implicits._
    import org.apache.spark.sql.expressions.scalalang._
    val cdr = inputDF.as[AllCaseClasses.CallDetailRecords]

    cdr.map(t => (t.squareId, t.timeInterval,
      t.countryCode, t.callInActivity+t.callOutActivity+t.smsInActivity+t.smsOutActivity+t.internetTrafficActivity,
      dateFromEpoch(t.timeInterval))).rdd
      .map(r => ((r._1,r._5), r._4))
      .reduceByKey(_ + _)
      .map(r => (r._1._1, r._1._2, r._2))
      .sortBy(_._3).toDF("squareId", "Date", "activity_per_day")
  }


  /**
    *  Task 4
    *
    *
    *  In this Function Spark Windows were used to create Seven Days Activity, and Mean all this window (7 Days Mean Activity)
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, HDFS input path to analyze Sqaure Id activity per day
    * @return Dataframe, Anomalous Activity of Square Id on basis of Seven Days Mean Activity
    *
    *  Example: Input Data
    *  squareId     timeinterval    country   smsInActivity smsOutActivity callInactivity callOutActivity internetActivity
    *
    *  1001         1385853000000   39        1.2           0.1             2.2           1.4             0.0
    *  1002         1385853000000   38        1.2           0.1             2.2           1.4             3.0
    *  1001         1385853600000   39        1.2           0.1             2.2           1.4             5.2
    *  1002         1385853600000   38        1.2           0.1             2.2           1.4             0.0
    *  ..
    *  ..
    *  ..
    *  .
    *
    *  And Result could be
    *
    *  squareId     dateStr                  date window                   seven_days_avg_activity     activity        anomalous
    *
    *  1001         2014-01-04             [2014-01-01, 2014-01-06]          x1                         x2               1
    *  1002         2014-01-10             [2014-01-07, 2014-01-13]          y1                         y2               0
    *  ..
    *  ..
    *  ..
    *  .
    *
    *
    */
  def findAnomalousData(spark: SparkSession, inputDF: DataFrame) = {
    import spark.implicits._
    val cdr = inputDF.as[AllCaseClasses.CallDetailRecords]

    val imediate = cdr.map(t => (t.squareId, t.timeInterval,
      t.callInActivity+t.callOutActivity+t.smsInActivity+t.smsOutActivity+t.internetTrafficActivity,
      epochToDate(t.timeInterval),extractHrFromEpoch(t.timeInterval))).rdd
      .map(t => ((t._1,t._4),t._3))
      .reduceByKey(_ + _)
      .map(s => (s._1._1, s._1._2, s._2))
      .sortBy(_._1).toDF("squareId","dateStr","total_activity")

    val task = imediate
      .groupBy($"squareId", window($"dateStr","1 week", "1 week","4 days"))
      .agg(mean($"total_activity").as("seven_days_avg_activity"))
      .select("squareId", "window.start", "window.end", "seven_days_avg_activity")

    imediate.join(task, (task("squareId") === imediate("squareId")) && (imediate("dateStr").cast("timestamp").geq(task("start")) || imediate("dateStr").cast("timestamp").leq(task("end"))), "left_outer")
      .drop(task("squareId"))
      .withColumn("anomalous", is_anomalous(col("total_activity"), col("seven_days_avg_activity")))
      .select("squareId", "dateStr", "start", "end", "total_activity", "seven_days_avg_activity", "anomalous")
  }

  /**
    *  Task 5
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, HDFS input path to analyze Sqaure Id activity per day
    * @return Dataframe, Daily basis activity of Square Id
    *
    *  Example: Input Data
    *  squareId     timeinterval    country   smsInActivity smsOutActivity callInactivity callOutActivity internetActivity
    *
    *  1001         1385853000000   39        1.2           0.1             2.2           1.4             0.0
    *  1002         1385853000000   38        1.2           0.1             2.2           1.4             3.0
    *  1001         1385853600000   39        1.2           0.1             2.2           1.4             5.2
    *  1002         1385853600000   38        1.2           0.1             2.2           1.4             0.0
    *  ..
    *  ..
    *  .
    *
    *  And Result could be
    *
    *  squareId     date          average_activity
    *
    *  1001         2014-01-01      xx
    *  1001         2014-01-02      xx1
    *  1001         2014-01-03      xx3
    *  1002         2014-01-01      yy
    *  1002         2014-01-11      yy1
    *  1002         2014-01-27      yy3
    *  ..
    *  ..
    *  ..
    *  .
    *
    */
  def averageActivitySquareId(spark: SparkSession, inputDF: DataFrame): DataFrame = {
    import spark.implicits._
    val cdr = inputDF.as[AllCaseClasses.CallDetailRecords]

    cdr.map(t => (t.squareId, t.callInActivity+t.callOutActivity+t.smsInActivity+t.smsOutActivity+t.internetTrafficActivity,
      epochToDate(t.timeInterval))).toDF("squareId", "total_activity", "date").groupBy("squareId", "date")
      .agg(avg("total_activity").as("average_activity"))

  }

  /**
    *  Task 6
    *
    * @param spark: SparkSession, It passed from main method
    * @param inputPath, HDFS input path to analyze Sqaure Id activity per day
    * @return Dataframe, Daily basis activity of Square Id
    *
    *  Example: Input Data
    *  squareId     timeinterval    country   smsInActivity smsOutActivity callInactivity callOutActivity internetActivity
    *
    *  1001         1385853000000   39        1.2           0.1             2.2           1.4             0.0
    *  1002         1385853000000   38        1.2           0.1             2.2           1.4             3.0
    *  1001         1385853600000   39        1.2           0.1             2.2           1.4             5.2
    *  1002         1385853600000   38        1.2           0.1             2.2           1.4             0.0
    *  ..
    *  ..
    *  .
    *
    *  And Result could be
    *
    *  Taken some small assumptions
    *
    *  IF total_activity IS GREATER OR EQUAL TO 500 => high
    *  IF total_activity IN BETWEEN (300, 500) => medium
    *  IF total_activity IS LESS THAN OR EQUAL TO 0.0 => LOW
    *
    *  countryCode     date          hour     total_activity      country_category
    *
    *     38         2013-11-30      23            xx                   high
    *     38         2013-11-30      22            xx1                  medium
    *     38         2013-11-30      11            xx2                  high
    *     38         2013-11-30      09            xx3                  low
    *     38         2013-11-30      02            xx5                  high
    *     39         2014-01-01      04            yy                   low
    *     ..
    *     ..
    *     .
    *
    *
    */
  def countryCatOnHourly(spark: SparkSession, inputDF: DataFrame) = {
    import spark.implicits._
    val cdrDS = inputDF.as[AllCaseClasses.CallDetailRecords]

    cdrDS.map(t => (t.countryCode, t.callInActivity+t.callOutActivity+t.smsInActivity+t.smsOutActivity+t.internetTrafficActivity,
      epochToDate(t.timeInterval), extractHrFromEpoch(t.timeInterval))).rdd
      .map(s => ((s._1, s._3, s._4), s._2))
      .reduceByKey( _ + _)
      .map(a => (a._1._1, a._1._2, a._1._3, a._2, labelTheCountry(a._2))).toDF("countryCode", "date", "hour", "total_activity", "country_category")

  }

}
