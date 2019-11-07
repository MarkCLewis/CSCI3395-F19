package sparkml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer

object TempMLStuff extends App {
  val spark =
    SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(
    Array(
      StructField("day", IntegerType),
      StructField("dayOfYear", IntegerType),
      StructField("month", IntegerType),
      StructField("state", StringType),
      StructField("year", IntegerType),
      StructField("precip", DoubleType),
      StructField("tave", DoubleType),
      StructField("tmax", DoubleType),
      StructField("tmin", DoubleType)
    )
  )

  val data = spark.read
    .schema(schema)
    //option("inferSchema", "true").
    .option("header", "true")
    // option("dateFormat", "yyyyMMdd")
    .csv("data/SanAntonioTemps.csv")

  data.show()
  data.describe().show()

  val dataWithTime = data.withColumn("time", 'year + 'dayOfYear / 366.0)

  // val va = new VectorAssembler()
  //   .setInputCols(Array("time", "precip", "tave", "tmax", "tmin"))
  //   .setOutputCol("features")
  // // println(va.explainParams())
  // val dataWithBigFeature = va.transform(dataWithTime)
  // dataWithBigFeature.show(false)

  // val scaler = new StandardScaler()
  //   .setInputCol("features")
  //   .setOutputCol("scaledFeatures")
  // val scalerModel = scaler.fit(dataWithBigFeature)
  // val dataScaled = scalerModel.transform(dataWithBigFeature)
  // dataScaled.show()
  // dataScaled.select('scaledFeatures).show(false)

  // Find warming is SA
  // val timeVA = new VectorAssembler().setInputCols(Array("time")).setOutputCol("timeVect")
  // val dataWithTimeFeature = timeVA.transform(dataWithTime)

  // val lr = new LinearRegression()
  //   .setFeaturesCol("timeVect")
  //   .setLabelCol("tave")

  // val lrModel = lr.fit(dataWithTimeFeature)
  // println(lrModel.coefficients)
  // val fitData = lrModel.transform(dataWithTimeFeature)
  // // fitData.show()

  // // Find variation in San Antonio
  // val dataWithSinCos = dataWithTime.withColumn("sin", sin('time*math.Pi*2)).withColumn("cos", cos('time*math.Pi*2))
  // val sinVA = new VectorAssembler().setInputCols(Array("time", "sin", "cos")).setOutputCol("sinFeature")
  // val dataWithSinFeature = sinVA.transform(dataWithSinCos)
  // val sinLR = new LinearRegression().setFeaturesCol("sinFeature").setLabelCol("tave")
  // val sinModel = sinLR.fit(dataWithSinFeature)
  // println(sinModel.coefficients)

  // Stuff for clustering
  val va = new VectorAssembler()
    .setInputCols(Array("precip", "tave", "tmax", "tmin"))
    .setOutputCol("features")
  val dataWithBigFeature = va.transform(dataWithTime)
  dataWithBigFeature.show(false)

  val kmeans = new KMeans().setK(4)
  val kmeansModel = kmeans.fit(dataWithBigFeature)
  val dataWithClusters1 = kmeansModel.transform(dataWithBigFeature)
  dataWithClusters1.show()

  val pdata = dataWithClusters1.select('dayOfYear.as[Double], 'tave.as[Double], 'prediction.as[Double]).collect()
  val cg = ColorGradient(0.0 -> RedARGB, 1.0 -> GreenARGB, 2.0 -> BlueARGB, 3.0 -> MagentaARGB)
  val plot = Plot.simple(ScatterStyle(pdata.map(_._1), pdata.map(_._2), colors = cg(pdata.map(_._3))))
  SwingRenderer(plot, 800, 800, true)
  spark.sparkContext.stop()
}
