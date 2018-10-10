package com.dru

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SQLExample {
  /**
    * 1.1) Read retail data CSV(s)
    * 1.2) Calculate total sales & profit per country
    *
    * Then
    *
    * 2.1) Calculate how much money each customer spend in total
    * 2.2) Find the `best customer` for each country
    * ... `best customer` is the one that spend most money
    **/
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("SQL-example")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val retailSchema = StructType(Array(
      StructField("InvoiceNo", StringType, nullable = false),
      StructField("StockCode", StringType, nullable = false),
      StructField("Description", StringType),
      StructField("Quantity", IntegerType, nullable = false),
      StructField("InvoiceDate", TimestampType),
      StructField("UnitPrice", DecimalType.SYSTEM_DEFAULT, nullable = false),
      StructField("CustomerID", DoubleType),
      StructField("Country", StringType)
    ))

    val isCancelled = udf((_: String).startsWith("C"))


    val retailDF = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(retailSchema)
      .load("src/main/resources/retail-data/*.csv")

    retailDF.printSchema()

    val profitPerCountry = retailDF
      .where(!isCancelled($"InvoiceNo"))
      .groupBy($"Country".as("country"))
      .agg(
        sum($"Quantity").as("sales"),
        sum($"Quantity" * $"UnitPrice").as("profit")
      )
      .orderBy($"profit".desc)
      .as("profits_per_country")

    val moneySpendPerCustomer = retailDF
      .where($"CustomerID".isNotNull && !isCancelled($"InvoiceNo"))
      .groupBy($"CustomerID".as("customer_id"), $"Country")
      .agg(sum($"Quantity" * $"UnitPrice").as("money_spend"))
      .as("money_spend_per_customer")


    val bestCustomersPerCountry = moneySpendPerCustomer
      .withColumn("max_money_spend", max($"money_spend") over Window.partitionBy($"Country") as "max_money_spend")
      .where($"money_spend" === $"max_money_spend")
      .select($"customer_id", $"Country", $"money_spend")
      .as("best_customers_per_country")

    val finalResult = profitPerCountry
      .join(bestCustomersPerCountry, $"profits_per_country.Country" === $"best_customers_per_country.Country")
      .select(
        $"profits_per_country.Country",
        $"sales",
        $"profit",
        $"customer_id".as("best_customer_id"),
        $"money_spend".as("money_spend_by_best_customer")
      )
      .cache()

    finalResult.show(truncate = false, numRows = 100)
    finalResult
      .write
      .csv("src/main/resources/retail-out")
  }
}
