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
  }
}
