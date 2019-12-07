package org.task

import java.io.File

import org.scalatest.FlatSpec

import scala.io.Source

class SensorStatisticsMainTest extends FlatSpec {

  "The solution" should "get list of csv files" in {
    val source = getClass.getResource("/sensors-stats").getPath
    val csvList = SensorStatisticsMain.getAllCsv(source)
    assertResult(csvList.map(_.getName))(List("leader-1.csv", "leader-2.csv"))
  }

  "The solution" should "create union rdd" in {
    val csvList = SensorStatisticsMain.getAllCsv(getClass.getResource("/sensors-stats").getPath)
    val unionRdd = SensorStatisticsMain.createRdd(csvList)
    val result = unionRdd.collect
    assert(result.nonEmpty && result.size == 9)
  }

}
