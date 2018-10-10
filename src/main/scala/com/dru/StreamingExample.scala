package com.dru

import java.util.Properties
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import slick.jdbc.PostgresProfile.api._
import com.typesafe.config._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

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
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(5))
    ssc.checkpoint("spark-checkpoint")

    val configBC = ssc.sparkContext.broadcast(ConfigFactory.load())

    val filesStream: DStream[String] = {
      val sc = ssc.sparkContext
      ssc.queueStream[String](mutable.Queue[RDD[String]](
        sc.textFile("src/main/resources/retail-jsons/2010-12-01.json"),
        sc.textFile("src/main/resources/retail-jsons/2010-12-02.json")
      ))
    }

    val retailDataStream: DStream[RetailData] = filesStream.map { json =>
      implicit val formats = DefaultFormats
      read[RetailData](json)
    }

    val stateSpec = StateSpec
      .function { (stockCode: String, retailDataOpt: Option[RetailData], state: State[RetailData.ProductSummary]) =>
        retailDataOpt match {
          case None             =>
            println(s"No data received for product $stockCode")
          case Some(retailData) =>
            val updatedState =
              state.getOption()
                .map(_ mergeValue retailData)
                .getOrElse(RetailData.ProductSummary.fromData(retailData))

            if (!state.exists()) {
              println(s"Setting first state for product $stockCode")
            }
            state.update(updatedState)
        }
        stockCode -> state.getOption().getOrElse(RetailData.ProductSummary.Zero)
      }
      .initialState {
        import spark.implicits._
        spark
          .read
          .jdbc(
            configBC.value.getString("retails.url"),
            table = "product_summaries",
            properties = {
              val props = new Properties()
              props.put("user", configBC.value.getString("retails.user"))
              props.put("password", configBC.value.getString("retails.password"))
              props.put("Driver", configBC.value.getString("retails.driver"))
              props
            }
          )
          .as[(String, Long, BigDecimal)]
          .map { case (stockCode, itemsSold, totalProfit) => (stockCode, RetailData.ProductSummary(itemsSold, totalProfit)) }
          .rdd
      }

    val summariesStream: DStream[(String, RetailData.ProductSummary)] = retailDataStream
      .map(rd => rd.stockCode -> rd)
      .mapWithState(stateSpec)
      .cache()

    summariesStream.print(20)
    summariesStream.foreachRDD { summariesRDD =>
      summariesRDD.foreachPartition { partition =>
        implicit val ec: ExecutionContext = ExecutionContext.global
        val db = Database.forConfig("retails", configBC.value)
        try {
          partition.foreach { case (stockCode, RetailData.ProductSummary(itemsSold, totalProfit)) =>
            val insertSummary = db.run(ProductSummaryTable.insertOrUpdate((stockCode, itemsSold, totalProfit)))
            Await.result(insertSummary, 5.seconds)
          }
        } finally {
          db.close()
        }
      }
    }


    ssc.start()
    ssc.awaitTermination()
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
