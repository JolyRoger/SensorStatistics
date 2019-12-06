package org.task

import java.io.File

object SensorStatisticsMain extends App {

  val dir = "c:\\Users\\torquemada\\workspace\\sensors-stats\\"

  def getListOfFiles(dir: String, validSensorData: File => Boolean) = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(validSensorData).toList
    else List.empty[File]
  }

  def getAllCsv = getListOfFiles(dir, f => f.isFile && f.getName.endsWith(".csv"))


}
