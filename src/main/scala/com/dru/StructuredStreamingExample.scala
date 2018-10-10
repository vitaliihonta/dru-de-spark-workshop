package com.dru

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
  * 1) Create streaming Dataset from .json files
  * 2) Calculate total profit on 10 minutes time minute
  * 3) Write into console
  *   - Use different Output modes
  **/
object StructuredStreamingExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("SQL-example")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val retailSchema = StructType(Array(
      StructField("invoiceNo", StringType, nullable = false),
      StructField("stockCode", StringType, nullable = false),
      StructField("description", StringType),
      StructField("quantity", IntegerType, nullable = false),
      StructField("invoiceDate", TimestampType),
      StructField("unitPrice", DecimalType.SYSTEM_DEFAULT, nullable = false),
      StructField("customerId", StringType),
      StructField("country", StringType)
    ))

    val retailDF = spark
      .readStream
      .schema(retailSchema)
      .option("maxFilesPerTrigger", 1)
      .json("src/main/resources/retail-jsons/*.json")

    val isCancelled = udf((_: String).startsWith("C"))


    val profitsDF = retailDF
      .where(!isCancelled($"invoiceNo"))
      .withColumn("profit", $"quantity" * $"unitPrice")
      .groupBy(window($"invoiceDate", "10 minutes"))
      .agg(sum($"profit").as("total_profit"), sum($"quantity").as("items_sold"))

    profitsDF.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }
}
