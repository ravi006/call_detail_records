package com.organization.ts

import com.organization.ts.analysis.CDRAnalysis
import com.organization.ts.support.SparkSessionSupport
import com.organization.ts.utils.HadoopUtils.pathExists
import com.organization.ts.data.DataFromLocalToHDFS
import com.organization.ts.utils.Constants

case class Config(process: String = "", path: String = "")

object CDRAnalysisMain {

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config]("CDRAnalysisMain") {
      head("CDRAnalysis", "1.0.1")
      opt[String]('p', "process") required() valueName("<process>") action { (x, c) =>
        c.copy(process = x) } text("Process argument for CDR data")
      opt[String]('p', "path") required() valueName("<path>") action { (x, c) =>
        c.copy(path = x) } text("HDFS Path argument for CDR data")
    }

    val c = parser.parse(args, Config()) match {
      case Some(config) => config

      case None => sys.exit(-1)
    }

    val processId = c.process

    val pathHDFS = c.path

    println(processId)

    val spark = new SparkSessionSupport().getSparkSession

    import spark.implicits._

    if (processId == "data-copy"){
      if(!pathExists(pathHDFS)){

        println(processId)
        val copied = DataFromLocalToHDFS.copyfromLocalToHDFS(Constants.LOCAL_PATH, pathHDFS)
        println(copied)

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is Already Exist !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "pre-processing"){
      if(pathExists(pathHDFS)){
        println(processId)
        val (dataProcessing, totalInputCount, totalOutputCount) = CDRAnalysis.dataPreprocessing(spark, pathHDFS)
        dataProcessing.write.parquet(Constants.PRE_PROCESS_DATA_HDFS_PATH)
        dataProcessing.show(10, false)

        if(totalInputCount == totalOutputCount){
          println("*********************************  No Duplicate Rows *********************************************")
        } else {

          println(s"Total Input Rows Count ::::::  ${totalInputCount}")

          println(s"Total Output Rows Count ::::::  ${totalOutputCount}")
        }

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "all"){
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)

        val task1 = CDRAnalysis.peakHrFromDailyBasis(spark, inputDF)
        task1.write.parquet(Constants.TASK1_HDFS_PATH)

        val task2 = CDRAnalysis.hourlyUsagePerId(spark, inputDF)
        task2.write.parquet(Constants.TASK2_HDFS_PATH)

        val task3 = CDRAnalysis.squareIdRankPerDaily(spark, inputDF)
        task3.write.parquet(Constants.TASK3_HDFS_PATH)

        val task4 = CDRAnalysis.findAnomalousData(spark, inputDF)
        task4.write.parquet(Constants.TASK4_HDFS_PATH)

        val task5 = CDRAnalysis.averageActivitySquareId(spark, inputDF)
        task5.write.parquet(Constants.TASK5_HDFS_PATH)

        val task6 = CDRAnalysis.countryCatOnHourly(spark, inputDF)
        task6.write.parquet(Constants.TASK6_HDFS_PATH)

        println("All Tasks Completed !!!! :) ")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "task1") {
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)
        val task1 = CDRAnalysis.peakHrFromDailyBasis(spark, inputDF)
        task1.write.parquet(Constants.TASK1_HDFS_PATH)
        println("Task one Completed !!!!")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "task2") {
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)
        val task2 = CDRAnalysis.hourlyUsagePerId(spark, inputDF)
        task2.write.parquet(Constants.TASK2_HDFS_PATH)
        println("Task one Completed !!!!")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "task3") {
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)
        val task3 = CDRAnalysis.squareIdRankPerDaily(spark, inputDF)
        task3.write.parquet(Constants.TASK3_HDFS_PATH)
        println("Task one Completed !!!!")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "task4") {
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)
        val task4 = CDRAnalysis.findAnomalousData(spark, inputDF)
        task4.write.parquet(Constants.TASK4_HDFS_PATH)
        println("Task one Completed !!!!")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "task5") {
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)
        val task5 = CDRAnalysis.averageActivitySquareId(spark, inputDF)
        task5.write.parquet(Constants.TASK5_HDFS_PATH)
        println("Task one Completed !!!!")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else if(processId == "task6") {
      if(pathExists(pathHDFS)){
        println(processId)

        val inputDF = spark.read.parquet(pathHDFS)
        val task6 = CDRAnalysis.countryCatOnHourly(spark, inputDF)
        task6.write.parquet(Constants.TASK6_HDFS_PATH)
        println("Task one Completed !!!!")

      } else {
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
        println(s"HDFS Path ${pathHDFS} is not exists !!, Please give valid HDFS Path.")
        println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      }

    } else {
      println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
      println(s"Given Processor is not Proper ${processId} !!, Please give valid Processor Path.")
      println(":::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
    }


    spark.close()

  }

}
