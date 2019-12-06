package org.task

import java.io.File

object SensorStatisticsMain extends App {


  def getListOfFiles(dir: String, validSensorData: File => Boolean) = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) d.listFiles.filter(validSensorData).toList
    else List.empty[File]
  }

  def getAllCsv(dir: String) = getListOfFiles(dir, f => f.isFile && f.getName.endsWith(".csv"))

}
