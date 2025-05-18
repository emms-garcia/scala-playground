package playground

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Word Count")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val textFile = spark.read.textFile("src/main/resources/sample.txt")

    val wordCounts = textFile
      .flatMap(_.split("\\s+"))
      .groupByKey(identity)
      .count()

    wordCounts.show()

    spark.stop()
  }
}
