package machlearn
import org.apache.spark.sql.SparkSession
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer

object OceansRegression {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Linear Regression").master("local[*]").getOrCreate()
    import spark.implicits._
  
    spark.sparkContext.setLogLevel("WARN")

    val bottle = spark.read.option("header", true).option("inferSchema", "true").csv("data/Oceans/bottle.csv").cache()
    bottle.printSchema()
    bottle.show()
    bottle.summary().show()

    val cast = spark.read.option("header", true).option("inferSchema", "true").csv("data/Oceans/cast.csv").cache()
    cast.printSchema()
    cast.show()
    cast.summary().show()

    val latLon = cast.select('Lon_Dec.as[Double], 'Lat_Dec.as[Double]).collect()
    val plot = Plot.simple(ScatterStyle(latLon.map(_._1), latLon.map(_._2)))
    SwingRenderer(plot, 800, 800, true)

    spark.stop()
  }
}