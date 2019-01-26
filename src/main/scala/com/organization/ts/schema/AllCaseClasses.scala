package com.organization.ts.schema

import org.apache.spark.sql.types.StructType

object AllCaseClasses {

  case class CallDetailRecords(squareId: String, timeInterval: Long, countryCode: Int, smsInActivity: Double,
                               smsOutActivity: Double, callInActivity: Double, callOutActivity: Double,
                               internetTrafficActivity: Double)

  case class TotalActivityByDay(squarerootId: String, timeInterval: Long, countryCode: Int, total_activity: Double,
                                       date: String, hour: Int)

  case class CallDetailRecordsWithOption(squareId: String, timeInterval: Long, countryCode: Int, smsInActivity: Option[Double],
                               smsOutActivity: Option[Double], callInActivity: Option[Double], callOutActivity: Option[Double],
                               internetTrafficActivity: Option[Double])

//  case class WindowClass(squareId: Long, window: StructType[String], seven_days_avg_activity: Double)

}
