package yazeeh.test.examples.spark.book

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Usage: MnMcount <mnm_file_dataset>
 */
object MMs extends Serializable {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config(getSparkConf)
      .getOrCreate()
//
//    if (args.length < 1) {
//      print("Usage: MnMcount <mnm_file_dataset>")
//      sys.exit(1)
//    }
    // get the M&M data set file name
    val mnmFile = "/Users/yazeeh/Desktop/spark/HelloSpark/datasets/mnm_dataset.csv"
    // read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)
    // display DataFrame
    mnmDF.show(5, false)

    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show all the resulting aggregation for all the dates and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // find the aggregate count for California by filtering
    val caCountMnNDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show the resulting aggregation for California
    caCountMnNDF.show(10)
  }

  def getSparkConf: SparkConf = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("Hello Spark")
    sparkConfig.setMaster("local[3]")
    sparkConfig.set("spark.sql.shuffle.partitions", "2")
    sparkConfig.set("spark.driver.host", "127.0.0.1")
    sparkConfig.set("spark.driver.bindAddress", "127.0.0.1")
  }
}
// scalastyle:on println
