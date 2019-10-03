package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

case class TempData(day: Int, dayOfYear: Int, month: Int,
    state: String, year: Int, precip: Double, tave: Double,
    tmax: Double, tmin: Double)
    
case class Season(month: Int, season: String)

case class JoinedStuff(year:Int, season: String, tmax: Double)

object TempDataTypeSafe extends App {
  val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val data = spark.read.schema(Encoders.product[TempData].schema).
    option("header", "true").
    csv("data/SanAntonioTemps.csv").as[TempData]
    
  data.show()
  
  val seasons = List(
    Season(12, "winter"),
    Season(1, "winter"),
    Season(2, "winter"),
    Season(3, "spring"),
    Season(4, "spring"),
    Season(5, "spring"),
    Season(6, "summer"),
    Season(7, "summer"),
    Season(8, "summer"),
    Season(9, "fall"),
    Season(10, "fall"),
    Season(11, "fall"))

  val seasonDF = spark.createDataset(spark.sparkContext.parallelize(seasons))
  
  seasonDF.show()
  
  val joinedData = data.joinWith(seasonDF, data("month") === seasonDF("month"))
  
  joinedData.show()
  
  val summerData = joinedData.filter(t => t._2.season == "summer")
  
  summerData.show()
  
  val simplerData = joinedData.map { case (td, s) => 
    JoinedStuff(td.year, s.season, td.tmax) }
  
  simplerData.show()
  
  val highTemps = simplerData.select('tmax.as[Double])
  val highArray = highTemps.collect()

  spark.sparkContext.stop()

}