name := "iscx-ids-spark"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-mllib" % "1.6.2",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "com.databricks" %% "spark-xml" % "0.3.3"
)
