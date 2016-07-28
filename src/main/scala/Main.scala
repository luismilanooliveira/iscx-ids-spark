package iscx

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
                              .setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val datasetPath = "/var/spark/datasets/iscxids/labeled/"
    val days : Array[String] = Array(
      "TestbedJun12",
      "TestbedJun13",
      "TestbedJun14",
      "TestbedJun15-1",
      "TestbedJun15-2",
      "TestbedJun15-3",
      "TestbedJun16-1",
      "TestbedJun16-2",
      "TestbedJun16-3",
      "TestbedJun17-1",
      "TestbedJun17-2",
      "TestbedJun17-3")
    val xmlFiles = days.map(d => datasetPath + d + ".xml")
    // flowsDays.foreach(println)

    val zipped = days.zip(xmlFiles)
    val dataframes = zipped.map {d => sqlContext
                                     .read
                                     .format("com.databricks.spark.xml")
                                     .option("rowTag",d._1).load(d._2)
      }
    println(dataframes.length)
    // dataframes.foreach(println(_.count))
    sc.stop()
  }
}
