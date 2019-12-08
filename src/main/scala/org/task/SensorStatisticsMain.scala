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
    }
  }
  /*
      val c2 = charWord.combineByKey(
        str => Set(str),
        (lst: Set[String], str) => lst + str,
        (lst: Set[String], lst2: Set[String]) => lst ++ lst2
      )
  */
  def processRdd(pairRdd: RDD[(String, String)]) = {
    def nanFilter(str: String) = str.startsWith("N")

    val res = pairRdd.combineByKey(
      str =>
    )

    pairRdd.map(sensor => {
      val sensorName = sensor._1
      val sensorData = sensor._2.toList
      val nanAmount = sensorData.count(nanFilter)
      val total = sensorData.size
      val pureData = sensorData.withFilter(!nanFilter(_)).map(_.toLong)
      val max = if (pureData.isEmpty) 0 else pureData.max
      val min = if (pureData.isEmpty) 0 else pureData.min
      val avg = if (pureData.isEmpty) 0 else pureData.sum / (total - nanAmount)

      (sensorName, total, nanAmount, min, avg, max)
    })
  }

  def createStats =

}
