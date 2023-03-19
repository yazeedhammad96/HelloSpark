package yazeeh.test.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object HelloSpark extends Serializable {

  def main(args: Array[String]): Unit = {
    print("Started Hello, spark")

    if (args.length == 0) {
      print("No Args error")
      System.exit(0)
    }
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate();

    val testDF: DataFrame = loadSurveyDF(args(0), spark)
    //    scala.io.StdIn.readLine();


    spark.sql("CREATE DATABASE IF NOT EXISTS PEOPLE_DB")
    testDF.groupBy(spark_partition_id()).count().show()
    testDF.write
//      .format("parquet")
      .mode(SaveMode.Overwrite)
//      .option("path", "avro/testOutput")
//      .partitionBy("city")
      .saveAsTable("PEOPLE_DB.people_data_tbl")

    spark.catalog.listTables("PEOPLE_DB").show()
    //      hive(testDF)

    val less42: _root_.org.apache.spark.sql.Dataset[_root_.org.apache.spark.sql.Row] = partitionAndgetLess24(testDF)


    //    print(testDF.show())
    //    print(less42.show())
    //    scala.io.StdIn.readLine();
    //process
    print("Finished Hello, spark")
    spark.stop()
  }

  def partitionAndgetLess24(testDF: DataFrame) = {
    val partitioned = testDF.repartition(2)
    val less42 = partitioned.where("age < 24")
    print(less42.collect().mkString("->"))
    less42
  }

  def loadSurveyDF(fileName: String, spark: SparkSession) = {
    val testDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(fileName)
    testDF
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
