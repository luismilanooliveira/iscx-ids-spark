package iscx

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import utils.{loadISCX, initSpark}
import org.apache.spark.sql.Row


import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}



object RandomForest {
  def main(args: Array[String]) {
    val datasetPath = args match {
       case Array(p,_*) => p
       case _           => "/var/spark/datasets/iscxids/labeled/"
     }
    val (sc,sqlContext) = initSpark()
    // Array[(String, DataFrame)]
    val dataframes  = loadISCX(sqlContext,datasetPath)

    // take only two first octets

    val data = dataframes(0)._2

    // MinMax
    val (dstByMin, dstByMax) = data.agg(min($"totalDestinationBytes"), max($"totalDestinationBytes")).first match {
      case Row(x: Double, y: Double) => (x, y)
    }

    val scaledRange = lit(2) // Range of the scaled variable
    val scaledMin = lit(-1)  // Min value of the scaled variable
    val vNormalized = ($"totalDestinationBytes" - vMin) / (vMax - vMin) // v normalized to (0, 1) range

    val vScaled = scaledRange * vNormalized + scaledMin
    // /MinMax
    val filteredData = sqlContext.createDataFrame(data.map { row =>
          Row(
              row.get(0)  // tag
            , row.get(1)  // appName
            , row.getString(2).split("\\.").take(2).mkString(".")  // destination
            , row.get(5)  // destinationPort
            , row.get(6)  // destinationTCPFlagsDescription
            , row.get(7)  // direction
            , row.get(8)  // protocolName
            , row.getString(9).split("\\.").take(2).mkString(".")  // destination
            , row.get(12) // sourcePort
            , row.get(13) // sourceTCPFlagsDescription
            , row.get(14) // startDateTime
            , row.get(15) // stopDateTime
            , row.get(16) // totalDestinationBytes
            , row.get(17) // totalDestinationPackets
            , row.get(18) // totalSourceBytes
            , row.get(19) // totalSourcePackets
            )
    }, data.schema)

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("Tag")
      .setOutputCol("indexedLabel")
      .fit(data)

    // Transform the non-numerical features using the pipeline api
    val stringColumns = data.columns
      .filter(!_.contains("Payload"))
      .filter(!_.contains("total"))

    val transformers: Array[PipelineStage] = stringColumns
      .map(cname => new StringIndexer()
             .setInputCol(cname)
             .setOutputCol(s"${cname}_index")
    )

    val longColumns = data.columns.filter(_.contains("total"))

    // minMax
    // string vs long columns

    val assembler  = new VectorAssembler()
      .setInputCols((stringColumns
                      .map(cname => s"${cname}_index")) ++ longColumns)
      .setOutputCol("features")

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)

    // val pipeline = new Pipeline().setStages(stages)
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline

    val stages : Array[PipelineStage] =
      transformers :+ assembler :+ featureIndexer
    val pipeline = new Pipeline()
      .setStages(stages ++
                 Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)

    sc.stop()
  }


}
