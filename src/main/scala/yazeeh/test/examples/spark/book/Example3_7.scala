package yazeeh.test.examples.spark.book

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import yazeeh.test.examples.HelloSpark.getSparkConf

object Example3_7 {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .config(getSparkConf)
      .getOrCreate()

    //    if (args.length <= 0) {
    //      println("usage Example3_7 <file path to blogs.json>")
    //      System.exit(1)
    //    }
    // Get the path to the JSON file
    val jsonFile = "/Users/yazeeh/Desktop/spark/LearningSparkV2/chapter3/scala/data/blogs.json"
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    val blogsDFgt = blogsDF.select(expr("Hits > 10000"))
    blogsDFgt.show()

    // Use an expression to compute big hitters for blogs
    // This adds a new column, Big Hitters, based on the conditional expression
    blogsDF.withColumn("Big Hitters", expr("Hits > 10000")).show()
    blogsDF.withColumn("AuthorId", concat(col("First"), col("Last"), col("Id"))).select("AuthorId").show();
    blogsDF.withColumn("AuthorId", concat(col("First"), col("Last"), col("Id")))
      .orderBy(col("Hits").desc)
      //      .sort(col("Hits").desc)  the data is sorted on each partition individually and this is why the order in the
      //      output data is not guaranteed. On the other hand, orderBy() collects all the data into a single executor and then sorts them.
      .show();
    blogsDF.withColumn("AuthorId", concat(col("First"), col("Last"), col("Id")))
      .orderBy(col("Hits").desc).write.format("parquet").saveAsTable("BigHitters")
    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}