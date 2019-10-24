package sparkml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler

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

  val va = new VectorAssembler()
    .setInputCols(Array("time", "precip", "tave", "tmax", "tmin"))
    .setOutputCol("features")
  // println(va.explainParams())
  val dataWithBigFeature = va.transform(dataWithTime)
  dataWithBigFeature.show(true)

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
  val scalerModel = scaler.fit(dataWithBigFeature)
  val dataScaled = scalerModel.transform(dataWithBigFeature)
  dataScaled.show()

  spark.sparkContext.stop()
}
