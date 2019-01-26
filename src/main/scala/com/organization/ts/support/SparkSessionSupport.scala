package com.organization.ts.support

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.SparkSession

object SparkSessionSupport {
  private var _instance: SparkSessionSupport = null

  private var initialized: Boolean = false

  //  def instance() = {
  //    if (_instance == null) {
  //      throw new IllegalStateException("SparkSessionSupport is not yet initialized. Please initialize it before using.")
  //    }
  //    _instance
  //  }

  def init() = {
    if (!initialized) {
      _instance = new SparkSessionSupport()
      initialized = true
      System.out.println("SparkSessionSupport initialization complete..................................................")
    }
    if (_instance == null) {
      throw new IllegalStateException("SparkSessionSupport is not yet initialized. Please initialize it before using.")
    }
  }
}


class SparkSessionSupport {

  private var sparkSession: SparkSession = null

  init()

  private def init() = {
    if (sparkSession == null) {
      System.out.println("Initializing SparkSessionSupport.............................................................")
      val sparkConf = new SparkConf().setAppName("CDRAnalysis Spark Session Support")
      sparkSession = SparkSession.builder
        .config(sparkConf)
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .appName("CDRAnalysis Spark Session Support")
        .enableHiveSupport()
        .getOrCreate()
      System.out.println("SparkSessionSupport initialization complete..................................................")
    }
  }

  def registerSparkListener(sparkSession: SparkSession) = {
    sparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onStageCompleted(event: SparkListenerStageCompleted) = {
        println(s"Stage ${event.stageInfo.stageId} is done.")
      }
    })
  }

  def getSparkSession = {
    sparkSession
  }

}
