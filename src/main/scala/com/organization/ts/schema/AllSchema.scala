package com.organization.ts.schema

import org.apache.spark.sql.types
import org.apache.spark.sql.types._

object AllSchema {

  val schemaCDR = StructType(Array(
    StructField("squareId", StringType, true),
    StructField("timeInterval", LongType, true),
    StructField("countryCode", IntegerType, true),
    StructField("smsInActivity", DoubleType, true),
    StructField("smsOutActivity",  DoubleType, true),
    StructField("callInActivity", DoubleType, true),
    StructField("callOutActivity", DoubleType, true),
    StructField("internetTrafficActivity", DoubleType, true)
  ))

}
