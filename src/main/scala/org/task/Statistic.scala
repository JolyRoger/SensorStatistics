package org.task

import org.task.SensorStatisticsMain.CombineWithAvg

class Statistic(val processedFiles: Int,
                val totalMeasurements: Long,
                val failedMeasurements: Long,
                val stats: Array[(String, CombineWithAvg)]) {

  implicit def ~=(x: Double, y: Double, precision: Double): Boolean = (x - y).abs < precision

  def print {
    println(s"\nNum of processed files: $processedFiles")
    println(s"Num of processed measurements: $totalMeasurements")
    println(s"Num of failed measurements: $failedMeasurements")
    println(s"\nSensors with highest avg humidity:\n")
    println(s"sensor-id,min,avg,max")

    stats.foreach {
      stat =>
        println(s"${stat._1}," +
          s"${if (stat._2._3 > 100) "NaN" else stat._2._3}," +
          s"${if (~=(stat._2._4, 0d, 0.0001)) "NaN" else Math.round(stat._2._4)}," +
          s"${if (stat._2._5 < 0) "NaN" else stat._2._5}")
    }
  }
}
