package com.dru

import org.apache.spark.SparkConf
import scala.concurrent.{Await, ExecutionContext}
import slick.jdbc.PostgresProfile.api._
import org.apache.spark.sql.SparkSession

/**
  *
  * 1) Create discrete stream from files
  * 2) Parse file lines as JSON
  * 3) Calculate `ProductSummary` for each unique product (`stockCode`)
  *   - Use [[org.apache.spark.streaming.dstream.PairDStreamFunctions.mapWithState]]
  * 4) Save summaries into postgres
  *
  **/
object StreamingExample {

  case class RetailData(
                         invoiceNo: String,
                         stockCode: String,
                         description: Option[String],
                         quantity: Int,
                         unitPrice: BigDecimal,
                         customerId: Option[Double],
                         country: String
                       ) {
    def isCancelled: Boolean = invoiceNo.startsWith("C")
  }

  object RetailData {
    case class ProductSummary(itemsSold: Long, totalProfit: BigDecimal) {
      def mergeValue(retailData: RetailData): ProductSummary =
        combine(ProductSummary.fromData(retailData))

      def combine(that: ProductSummary): ProductSummary =
        ProductSummary(
          this.itemsSold + that.itemsSold,
          this.totalProfit + that.totalProfit
        )
    }

    object ProductSummary {
      def fromData(retailData: RetailData): ProductSummary =
        ProductSummary(retailData.quantity, retailData.quantity * retailData.unitPrice)

      val Zero: ProductSummary = ProductSummary(0, 0)
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Streaming-example")
      .setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  }
}


class ProductSummaryTable(tag: Tag) extends Table[(String, Long, BigDecimal)](tag, "product_summaries") {
  val stockCode   = column[String]("stock_code", O.PrimaryKey)
  val itemsSold   = column[Long]("items_sold")
  val totalProfit = column[BigDecimal]("total_profit")

  def * = (stockCode, itemsSold, totalProfit)
}

object ProductSummaryTable {
  val query = TableQuery[ProductSummaryTable]

  def insertOrUpdate(rec: (String, Long, BigDecimal))(implicit ec: ExecutionContext) = {
    val sameCodeQuery = query.filter(_.stockCode === rec._1)
    sameCodeQuery.exists.result.flatMap {
      case false => query += rec
      case _     => sameCodeQuery.update(rec)
    }
  }
}
