import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import breeze.util.BloomFilter

object SparkApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(SparkApp.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext

    val words = sc
      .textFile("./data/words.txt")
    
    val numItems = words.count()
    val falsePositiveRate = 0.01
    val bf = BloomFilter.optimallySized[String](numItems, falsePositiveRate)

    words.collect().foreach(word => bf.+=(word))

    val falsePositives = words.map(word => { if (bf.contains(word)) 1 else 0 }).count()
    val empiricalFPR = falsePositives.toDouble / numItems

    println("==== BLOOM FILTER STATS ====")
    println(s"Number of false positives: $falsePositives")
    println(f"Empirical false positive rate: $empiricalFPR%.4f%%")
    println("==== END STATS ====")

    spark.stop()
  }
}
