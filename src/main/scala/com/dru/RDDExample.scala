package com.dru

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

object RDDExample {

  /**
    * 1) Read text file
    * 2) Extract all words
    * 3) Count unique words
    **/
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("RDD-example")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(sparkConf)

    val fileLinesRDD: RDD[String] = sc.textFile("src/main/resources/text.txt")

    val blacklist = sc.broadcast(
      Set(
        "a", "and", "of", "to", "the",
        "in", "are", "is", "as", "by",
        "on", "or", "not", "have", "can",
        "be", "can"
      )
    )

    val wordsRDD = fileLinesRDD
      .flatMap(_.split("(\\s|,|\\.)"))
      .map(_.toLowerCase)
      .filter(_.matches("[\\w]+"))
      .filter(!blacklist.value.contains(_))

    val wordsFrequencyRDD =
      wordsRDD
        .map((_, 1))
        .reduceByKey(_ + _)
        .filter(_._2 > 1)
        .sortBy(_._2, ascending = false)

    println(wordsFrequencyRDD.collect().mkString("\n"))

    sc.stop()
  }
}
