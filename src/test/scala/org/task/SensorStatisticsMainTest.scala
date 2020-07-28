package org.task

import org.scalatest.FlatSpec

class SensorStatisticsMainTest extends FlatSpec {

  "The solution" should "get list of csv files" in {
    val source = getClass.getResource("/sensors-stats").getPath
    val csvList = SensorStatisticsMain.getAllCsv(source)
    assertResult(csvList.map(_.getName))(List("leader-1.csv", "leader-2.csv"))
  }

  "The solution" should "create union rdd" in {
    val csvList = SensorStatisticsMain.getAllCsv(getClass.getResource("/sensors-stats").getPath)
    val (unionRdd, _) = SensorStatisticsMain.createRdd(csvList)
    val result = unionRdd.collect
    assert(result.nonEmpty && result.length == 9)
  }

  "The solution" should "create and filter pair rdd" in {
    val csvList = SensorStatisticsMain.getAllCsv(getClass.getResource("/sensors-stats").getPath)
    val (unionRdd, headers) = SensorStatisticsMain.createRdd(csvList)
    val pairRdd = SensorStatisticsMain.createDataRdd(unionRdd, headers)
    val result = pairRdd.collect
    assert(result.length == 7)
  }

  "The solution" should "convert data rdd to stats rdd" in {
    val csvList = SensorStatisticsMain.getAllCsv(getClass.getResource("/sensors-stats").getPath)
    val (unionRdd, headers) = SensorStatisticsMain.createRdd(csvList)
    val pairRdd = SensorStatisticsMain.createDataRdd(unionRdd, headers)
    val stats = SensorStatisticsMain.processRdd(pairRdd)
    val result = stats.collect
    assert(result.length == 3)
  }

  "The solution" should "collect statistics" in {
    val stats = SensorStatisticsMain.collectStats(getClass.getResource("/sensors-stats").getPath)
    assert(stats.totalMeasurements == 7 && stats.failedMeasurements == 2 && stats.processedFiles == 2)
  }
}
