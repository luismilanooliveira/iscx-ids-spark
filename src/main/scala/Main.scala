package iscx

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SimpleApp {
   def main(args: Array[String]) {

     val datasetPath = args match {
       case Array(p,_*) => args(0)
       case _           => "/var/spark/datasets/iscxids/labeled/"
     }
    val (sc,sqlContext) = initSpark()

    val dataframes = loadISCX(sqlContext,datasetPath)
    // dataframes.foreach((d : DataFrame) => println(d.count))

    sc.stop()
  }

  def initSpark() : (SparkContext,SQLContext)= {
    val conf = new SparkConf().setAppName("Simple Application")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    (sc,sqlContext)
  }

  def loadISCX(sqlContext : SQLContext, path: String) : Array[DataFrame] = {
    val days : Array[String] = Array(
      "TestbedSatJun12",
      "TestbedSunJun13",
      "TestbedMonJun14",
      "TestbedTueJun15-1",
      "TestbedTueJun15-2",
      "TestbedTueJun15-3",
      "TestbedWedJun16-1",
      "TestbedWedJun16-2",
      "TestbedWedJun16-3",
      "TestbedThuJun17-1",
      "TestbedThuJun17-2",
      "TestbedThuJun17-3")

    val xmlFiles = days.map(d => path + d + ".xml")
    val zipped = days.map(_ + "Flows").zip(xmlFiles)

    zipped.map {d => sqlContext
                              .read
                              .format("com.databricks.spark.xml")
                              .option("rowTag",d._1).load(d._2)
    }
    // TestbedJun12
    // val jun12 = sqlContext.read
    //   .format("com.databricks.spark.xml")
    //   .option("rowTag",days(0))
    //   .load(xmlFiles(0))
    // // TestbedJun13
    // val jun13 = sqlContext.read
    //   .format("com.databricks.spark.xml")
    //   .option("rowTag",days(1) + "Flows")
    //   .load(xmlFiles(1))
    // // TestbedJun14
    // val jun14 = sqlContext.read
    //   .format("com.databricks.spark.xml")
    //   .option("rowTag",days(2) + "Flows")
    //   .load(xmlFiles(2))
    // // TestbedJun15
    // val jun15 = sqlContext.read
    //   .format("com.databricks.spark.xml")
    //   .option("rowTag",days(3) + "Flows")
    //   .load(xmlFiles(3))
    // // TestbedJun16
    // val jun16 = sqlContext.read
    //   .format("com.databricks.spark.xml")
    //   .option("rowTag",days(4) + "Flows")
    //   .load(xmlFiles(4))
    // // TestbedJun17
    // val jun13 = sqlContext.read
    //   .format("com.databricks.spark.xml")
    //   .option("rowTag",days(1) + "Flows")
    //   .load(xmlFiles(2))
  }
}
