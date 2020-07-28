package org.task

import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** Core idea to create united Spark RDD from multiple csv files and collect some
  * statistics using Spark methods.
  */
object SensorStatisticsMain {

  /** Represents (total measurement number, failed measurement number, min, sum, max) */
  type CombineWithSum = (Long, Long, Int, Long, Int)

  /** Represents (total measurement number, failed measurement number, min, average, max) */
  type CombineWithAvg = (Long, Long, Int, Double, Int)

  def getListOfFiles(dir: String, validSensorData: File => Boolean): List[File] = {
    val d = new File(dir)
    d.listFiles.filter(validSensorData).toList
  }

  def getAllCsv(dir: String): List[File] = getListOfFiles(dir, f => f.isFile && f.getName.endsWith(".csv"))

  def createRdd(csv: List[File]): (RDD[String], Set[String]) = {
    val conf = new SparkConf().setAppName("SensorsStatistics").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

    val fileRdd = csv.map(f => sc.textFile(f.getAbsolutePath))
    val headers = fileRdd.flatMap(_.take(1)).toSet
    (sc.union(fileRdd), headers)
  }

  /**
    * It creates united RDD from all csv files like:
    * s1,82
    * s3,78
    * s2,56
    * s1,79
    * etc
    *
    * @param rdd RDD of string with comma in the middle
    * @param headers Set of headers. Not a result of measurement
    * @return Pair RDD without headers
    */
  def createDataRdd(rdd: RDD[String], headers: Set[String]): RDD[(String, String)] = {
    rdd.collect { case record if !headers.contains(record) =>
      val recArr = record.split(",")
      (recArr(0).trim, recArr(1).trim)
    }
  }

  def nanFilter(str: String): Boolean = str.startsWith("N")

  def initializer(str: String): (Long, Long, Int, Long, Int) = if (nanFilter(str)) (1L, 1L, 101, 0L, -1) else {
    val num = str.toInt
    (1L, 0L, num, num.toLong, num)
  }

  def merger(statUnit: CombineWithSum, data: String): (Long, Long, Int, Long, Int) =
      if (nanFilter(data)) (statUnit._1 + 1, statUnit._2 + 1, statUnit._3, statUnit._4, statUnit._5) else {
      val num = data.toInt
      (statUnit._1 + 1, statUnit._2, Math.min(statUnit._3, num), statUnit._4 + num, Math.max(statUnit._5, num))
    }

  def combiner(statUnit1: CombineWithSum, statUnit2: CombineWithSum): (Long, Long, Int, Long, Int) =
    (statUnit1._1 + statUnit2._1, statUnit1._2 + statUnit2._2, Math.min(statUnit1._3, statUnit2._3), statUnit1._4 + statUnit2._4,
      Math.max(statUnit1._5, statUnit2._5))

  /**
    * Transforms original RDD with raw data measurements to another RDD combining data
    * @param pairRdd Raw measurement data RDD. (sensor,value)
    * @return Statistic RDD (sensor,(total measurements, failed measurements, min, average, max))
    */
  def processRdd(pairRdd: RDD[(String, String)]): RDD[(String, (Long, Long, Int, Double, Int))] = {
    pairRdd.combineByKey(
      initializer,
      merger,
      combiner
    ).mapValues(v => (v._1, v._2, v._3,
      if (v._1 - v._2 == 0) 0d else
        v._4 / (v._1 - v._2).toDouble,
      v._5))
  }

  def collectStats(path: String): Statistic = {
    val csvList = getAllCsv(path)
    val processedFileSize = csvList.size
    val (unionRdd, headers) = createRdd(csvList)
    val pairRdd = createDataRdd(unionRdd, headers)
    val stats = processRdd(pairRdd).cache

    val (totalMeasurements, failedMeasurements) = stats.values.aggregate(0L, 0L)(
      (newData, data) => (data._1 + newData._1, data._2 + newData._2),
      (data1, data2) => (data1._1 + data2._1, data1._2 + data2._2))

    new Statistic(processedFileSize, totalMeasurements,
      failedMeasurements, stats.collect.sortBy(_._2._4)(Ordering[Double].reverse))
  }

  def main(args: Array[String]) {
    if (args.isEmpty) throw new IllegalArgumentException("Directory path missing")
    val f = new File(args(0))
    if (!(f.exists && f.isDirectory))
      throw new IllegalArgumentException(s"File ${f.getAbsoluteFile} does not exist or is not a directory")
    val stats = collectStats(args(0))
    stats.print
  }
}
