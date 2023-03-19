package yazeeh.test.examples.spark.book

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import yazeeh.test.examples.spark.book.MMs.getSparkConf

object FireDepartment {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .config(getSparkConf)
      .getOrCreate()

    val fireDF = spark.read.option("header", true).option("inferSchema", true).csv("/Users/yazeeh/Desktop/spark/HelloSpark/datasets/fireDp.csv")
    println("--------------------------------")

    //    usingDistinctAndAlias(fireDF)
    //    usingWhereAndColRenamed(fireDF)
    //    usingToTimestamp(fireDF)
    //    usingCountAndMiscOperations(fireDF)

  }

  private def usingCountAndMiscOperations(fireDF: DataFrame): Unit = {
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    fireDF
      .select(sum("NumAlarms").alias("Sum of Num Alarms"), avg("NumAlarms") as "Avg of Num Alarms",
        min("NumAlarms"), max("NumAlarms"))
      .show()
  }

  private def usingToTimestamp(fireDF: DataFrame): Unit = {
    val fireTsDF = fireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    // Select the converted columns
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct()
      .orderBy(year(col("IncidentDate")))
      .show()


  }

  private def usingWhereAndColRenamed(fireDF: DataFrame): Unit = {
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins") //It will drop Delay col

    newFireDF
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins") > 5)
      .show(5, false)
    println("--------------------------------")

    newFireDF
      .select("ResponseDelayedinMins")
      .where(expr("ResponseDelayedinMins") > 5)
      .show(5, false)
    println("--------------------------------")

    newFireDF
      .selectExpr("ResponseDelayedinMins > 5")
      .show(5, false)
    println("--------------------------------")

    newFireDF.show(5)
  }

  private def usingDistinctAndAlias(fireDF: DataFrame): Unit = {
    val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType") =!= "Medical Incident")
    fewFireDF.show(5)
    println("--------------------------------")


    println(fireDF.select("CallType").distinct().count())

    println("--------------------------------")
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()
    println("--------------------------------")

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType")).alias("DistinctCallTypes")
      .show()
  }
}
