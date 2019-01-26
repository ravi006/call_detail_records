package com.organization.ts.data

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object DataFromLocalToHDFS {

  def copyfromLocalToHDFS(sourcePath: String, distPath: String): String = {
    try {
      val hadoopConf = new Configuration()
      val hdfs = FileSystem.get(hadoopConf)

      val srcPath = new Path(sourcePath)
      val destPath = new Path(distPath)
      hdfs.copyFromLocalFile(srcPath, destPath)
      "Successfully completed !!, copy local files to HDFS"

    } catch {
      case ex: FileNotFoundException => {
        s"File $sourcePath not found"
      }
      case unknown: Exception => {
        s"Unknown exception: $unknown"
      }
    }
  }

}
