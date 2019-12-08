package org.task

import java.io.File
import java.math.{BigDecimal, BigInteger, RoundingMode}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SensorStatisticsMain extends App {

  type CombineWithSum = Tuple5[Int, Int, Int, BigInteger, Int]

  implicit def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }

  def getListOfFiles(dir: String, validSensorData: File => Boolean) = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(validSensorData).toList
    else List.empty[File]
  }

  def getAllCsv(dir: String) = getListOfFiles(dir, f => f.isFile && f.getName.endsWith(".csv"))

  def createRdd(csv: List[File]) = {
    val conf: SparkConf = new SparkConf().setAppName("SensorsStatistics").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

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

  def nanFilter(str: String) = str.startsWith("N")

  def initializer(str: String) = if (nanFilter(str)) (1, 1, 101, BigInteger.ZERO, -1) else {
      val num = str.toInt
      (1, 0, num, BigInteger.valueOf(num), num)
    }

  def merger(statUnit: CombineWithSum, data: String) =
    if (nanFilter(data)) (statUnit._1 + 1, statUnit._2 + 1, statUnit._3, statUnit._4, statUnit._5) else {
      val num = data.toInt
      (statUnit._1 + 1, statUnit._2, Math.min(statUnit._3, num), statUnit._4.add(BigInteger.valueOf(num)), Math.max(statUnit._5, num))
    }

  def combiner(statUnit1: CombineWithSum, statUnit2: CombineWithSum) =
      (statUnit1._1 + statUnit2._1, statUnit1._2 + statUnit2._2, Math.min(statUnit1._3, statUnit2._3), statUnit1._4.add(statUnit2._4),
        Math.max(statUnit1._5, statUnit2._5))

  def processRdd(pairRdd: RDD[(String, String)]) = {
    pairRdd.combineByKey(
      initializer,
      merger,
      combiner
    ).mapValues(v => (v._1, v._2, v._3,
      if (v._1 - v._2 == 0) 0d else
      new BigDecimal(v._4).divide(new BigDecimal(BigInteger.valueOf(v._1 - v._2)), 3, RoundingMode.HALF_EVEN).doubleValue,
      v._5))
  }

  def collectStats(path: String) = {
    val csvList = getAllCsv(path)
    val processedFileSize = csvList.size
    val (unionRdd, headers) = createRdd(csvList)
    val pairRdd = createDataRdd(unionRdd, headers)
    val stats = processRdd(pairRdd).cache

    val (totalMeasurements, failedMeasurements) = stats.values.aggregate(0, 0)(
      (newData, data) => (data._1 + newData._1, data._2 + newData._2),
      (data1, data2) => (data1._1 + data2._1, data1._2 + data2._2))

    println(s"Num of processed files: $processedFileSize")
    println(s"Num of processed measurements: $totalMeasurements")
    println(s"Num of failed measurements: $failedMeasurements")

    println(s"\nSensors with highest avg humidity:\n")
    println(s"sensor-id,min,avg,max")

    val sortedStat = stats.collect.sortBy(_._2._4)(Ordering[Double].reverse)

    sortedStat.foreach {
      stat =>
        println(s"${stat._1}," +
          s"${if (stat._2._3 > 100) "NaN" else stat._2._3}," +
          s"${if (~=(stat._2._4, 0d, 0.0001)) "NaN" else stat._2._4}," +
          s"${if (stat._2._5 < 0) "NaN" else stat._2._5}")
    }
  }
}
