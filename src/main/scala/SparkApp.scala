import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(SparkApp.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext

    println("Spark App")

    spark.stop()
  }
}

