package sparkml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object ClassifyAdults {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("Temp Data")
        .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val rawData = spark.read.option("header", "true").option("inferSchema", "true").csv("data/adult.csv")

    rawData.show()
    rawData.printSchema()

    val withNumIncome = rawData.withColumn("numIncome", when('income === ">50K", 1).otherwise(0))

    val intFeatures = "age educationNum capitalGain capitalLoss hoursPerWeek".split(" ")
    val stringFeatureCols = "workclass maritalStatus occupation relationship race sex".split(" ")
    val indexedData = stringFeatureCols.foldLeft(withNumIncome) {(ds, name) =>
      val indexer = new StringIndexer().setInputCol(name).setOutputCol(name+"-i")
      indexer.fit(ds).transform(ds).drop(name)
    }

    val va = new VectorAssembler().setInputCols(intFeatures ++ stringFeatureCols.map(_+"-i")).setOutputCol("features")
    val vectData = va.transform(indexedData)

    val Array(trainData, validData) = vectData.randomSplit(Array(0.8, 0.2)).map(_.cache())

    val randomForest = new RandomForestClassifier().setLabelCol("numIncome")
    val rfModel = randomForest.fit(trainData)

    val predictions = rfModel.transform(validData)
    predictions.show()
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("numIncome")
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy = "+accuracy)

    spark.stop()
  }
}
