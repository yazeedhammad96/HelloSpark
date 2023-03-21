package yazeeh.test.examples.ch4

import org.apache.spark.sql.SparkSession
import yazeeh.test.examples.spark.book.ch3.MMs.getSparkConf

object UsingSqlUDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .config(getSparkConf)
      .getOrCreate()

    val cubed = (s: Long) => {
      s * s * s
    }

    // Register UDF
    spark.udf.register("cubed", cubed)

    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

  }

}
