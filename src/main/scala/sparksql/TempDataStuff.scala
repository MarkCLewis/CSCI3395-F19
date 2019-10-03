package sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sqrt
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import swiftvis2.plotting.Plot
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer

object TempDataStuff extends App {
  val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val schema = StructType(Array(
    StructField("day", IntegerType),
    StructField("dayOfYear", IntegerType),
    StructField("month", IntegerType),
    StructField("state", StringType),
    StructField("year", IntegerType),
    StructField("precip", DoubleType),
    StructField("tave", DoubleType),
    StructField("tmax", DoubleType),
    StructField("tmin", DoubleType)))

  val data = spark.read.schema(schema).
    //option("inferSchema", "true").
    option("header", "true").
    // option("dateFormat", "yyyyMMdd")
    csv("data/SanAntonioTemps.csv")

  data.show()
  data.describe().show()

  val seasons = List(
    Row(12, "winter"),
    Row(1, "winter"),
    Row(2, "winter"),
    Row(3, "spring"),
    Row(4, "spring"),
    Row(5, "spring"),
    Row(6, "summer"),
    Row(7, "summer"),
    Row(8, "summer"),
    Row(9, "fall"),
    Row(10, "fall"),
    Row(11, "fall"))

  val seasonSchema = StructType(Array(
    StructField("month", IntegerType),
    StructField("season", StringType)))

  val seasonDF = spark.createDataFrame(spark.sparkContext.parallelize(seasons), seasonSchema)

  data.select(('day + ('month - 1) * 31 === 'dayOfYear), sqrt('day)).show()
  data.agg(count($"day")).show()
  println(data.stat.corr("precip", "tmax"))

  data.createOrReplaceTempView("sadata")
  val rainy = spark.sql("""
    SELECT * FROM sadata 
    WHERE precip>0.1
  """)
  rainy.show()

  val monthlyData = data.groupBy('month)
  val aveMonthlyTemps = monthlyData.agg(avg('tmax))
  aveMonthlyTemps.orderBy('month).show()

  val joinedData = data.join(seasonDF, "month")
  joinedData.groupBy('season).agg(avg('tmax)).show()

  val xy = data.select('dayOfYear, 'tmax).collect()
  val plot = Plot.scatterPlot(xy.map(_.getDouble(0)), xy.map(_.getDouble(1)), "All The Temps", "Day of Year", "Degrees")
  SwingRenderer(plot)
  
  spark.sparkContext.stop()
}