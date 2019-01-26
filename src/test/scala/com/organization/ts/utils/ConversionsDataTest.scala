package com.organization.ts.utils


import com.organization.ts.utils.ConversionsData.{convertOptions, labelTheCountry, isAnomalous}

import com.organization.SparkSessionTestWrapper
import org.scalatest.{FlatSpec, Matchers}

class ConversionsDataTest extends FlatSpec with Matchers with SparkSessionTestWrapper {
  import spark.implicits._

  "convertOptions" should "Convert Optional values to Double valid values" in {

    val sourceValue = convertOptions(null)

    val actualValue = 0.0

    assert(sourceValue === actualValue)
  }


  "labelTheCountry" should "Convert Double value to Range String" in {

    val sourceValue = labelTheCountry(501)

    val actualValue = Constants.LABEL_HIGH

    assert(sourceValue === actualValue)
  }

  "labelTheCountry" should "Convert Double value to MEDIUM Range String" in {

    val sourceValue = labelTheCountry(200)

    val actualValue = Constants.LABEL_MEDIUM

    assert(sourceValue === actualValue)
  }


  "isAnomalous" should "Boolean value 1 or 0" in {

    val sourceValue = isAnomalous(123, 456)

    println(sourceValue)
    val actualValue = 1

    assert(sourceValue === actualValue)
  }

}
