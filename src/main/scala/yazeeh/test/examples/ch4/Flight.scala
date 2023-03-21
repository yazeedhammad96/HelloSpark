package yazeeh.test.examples.ch4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import yazeeh.test.examples.spark.book.ch3.MMs.getSparkConf

object Flight {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .config(getSparkConf)
      .getOrCreate()

    val delaysPath =
      "src/main/scala/yazeeh/test/examples/flights/departuredelays.csv"
    val airportsPath =
      "src/main/scala/yazeeh/test/examples/flights/airport-codes-na.txt"

    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports")

    // Obtain departure Delays data set
    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table
    val foo = delays.filter(
      expr(
        """origin == 'SEA' AND destination == 'SFO' AND
          date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports LIMIT 10").show()
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    spark.sql("SELECT * FROM foo").show()

    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr(
      """origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()

    foo.join(
      airports.as('air),
      col("air.IATA") === col("origin")
    ).select("City", "State", "date", "delay", "distance", "destination").show()

  }

}
