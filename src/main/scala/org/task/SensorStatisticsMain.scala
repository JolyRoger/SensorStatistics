package org.task

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SensorStatisticsMain extends App {

  def getListOfFiles(dir: String, validSensorData: File => Boolean) = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(validSensorData).toList
    else List.empty[File]
  }

  def getAllCsv(dir: String) = getListOfFiles(dir, f => f.isFile && f.getName.endsWith(".csv"))

  def createRdd(csv: List[File]) = {
    val conf: SparkConf = new SparkConf().setAppName("SensorsStatistics").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val fileRdd = csv.map(f => sc.textFile(f.getAbsolutePath))
    val headers = fileRdd.flatMap(_.take(1)).toSet
    (sc.union(fileRdd), headers)
  }

  def createDataRdd(rdd: RDD[String], headers: Set[String]) = {
    rdd.collect { case record if !headers.contains(record) =>
      val recArr = record.split(",")
      (recArr(0).trim, recArr(1).trim)
    }.groupByKey
  }
}
