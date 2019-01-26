package com.organization.ts.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object ConversionsData {

  def convertOptions(s1: Option[Double]): Double = s1 match {
    case Some(d) => d
    case _ => 0.0
  }

  def labelTheCountry(input: Double) = input match {
    case r if r > 500 => Constants.LABEL_HIGH
    case r if r > 100 | 500 >= r => Constants.LABEL_MEDIUM
    case r if r > 0.0 | 100 >= r => Constants.LABEL_LOW
    case _ => Constants.LABEL_UNKNOWN
  }

  def is_anomalous =  udf[Int, Double, Double](isAnomalous)

  def isAnomalous(total: Double, avg: Double) = if(total < avg) 1 else 0

}
