package org.task

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.hadoop.util.Time.now
import scala.util.Random

object Generator extends App {

  def writeFile(filename: String, lines: Seq[String]) {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("sensor,humidity\n")
    for (line <- lines) {
      bw.write(line + '\n')
    }
    bw.close()
  }
  val nrange = 1 to 50
  val sensorNumber = new Random(now)
  val measurement = new Random(now / 11)

  for (nfile <- nrange) {
    val col = for (i <- 1 to 1000000;
                   sensorNum = sensorNumber.nextInt(100);
                   result = measurement.nextInt(115)) yield
      s"s$sensorNum,${if (result > 100) "NaN" else result}"

    writeFile("C:\\Users\\torquemada\\workspace\\sensor-statistics-data\\leader-" + nfile + ".csv", col)
  }
}
