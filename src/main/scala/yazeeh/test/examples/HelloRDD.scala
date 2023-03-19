package yazeeh.test.examples

import org.apache.spark.{SparkConf, SparkContext}

object HelloRDD extends Serializable {

  def main(args: Array[String]): Unit = {
    print("Started Hello, spark")

    //    if (args.length == 0) {
    //      print("No Args error")
    //      System.exit(0)
    //    }
    val spark = new SparkContext(getSparkConf)

    val linesRDD = spark.textFile("test.csv")
    val repartitioned = linesRDD.repartition(2)

    val colsRDD = repartitioned.map(line => line.split(",").map(_.trim))
    val selectRDD = colsRDD.map(cols => SurveyRecord(cols(0).toInt, cols(1), cols(2).toInt, cols(3)))
    val filteredRDD = selectRDD.filter(row => row.age < 40)
    val kvRDD = filteredRDD.map(row => (row.city, 1))
    val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

    print(countRDD.collect().mkString(","))
    spark.stop()
  }


  def getSparkConf: SparkConf = {
    val sparkConfig = new SparkConf()
    sparkConfig.setAppName("Hello Spark")
    sparkConfig.setMaster("local[3]")
    sparkConfig.set("spark.sql.shuffle.partitions", "2")
    sparkConfig.set("spark.driver.host", "127.0.0.1")
    sparkConfig.set("spark.driver.bindAddress", "127.0.0.1")
  }

  case class SurveyRecord(id: Int, name: String, age: Int, city: String)
}
