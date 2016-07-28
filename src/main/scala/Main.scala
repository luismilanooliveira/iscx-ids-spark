package iscx

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.databricks.spark.xml._

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
                              .setMaster("local[4]")
    val sc = new SparkContext(conf).setLogLevel("WARN")

    val datasetPath = "/var/spark/datasets/iscxids/labeled/"
    val days : Array[String] = Array(
      "TestbedJun12.xml",
      "TestbedJun13.xml",
      "TestbedJun14.xml",
      "TestbedJun15-1.xml",
      "TestbedJun15-2.xml",
      "TestbedJun15-3.xml",
      "TestbedJun16-1.xml",
      "TestbedJun16-2.xml",
      "TestbedJun16-3.xml",
      "TestbedJun17-1.xml",
      "TestbedJun17-2.xml",
      "TestbedJun17-3.xml")
    val flows = days.map(d => datasetPath + d)
    flows.foreach(println)
  }
}
