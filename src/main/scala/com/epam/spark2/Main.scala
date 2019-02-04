package com.epam.spark2

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * start point*/
object Main extends App with Engine {

  println(args.mkString("\n"))

  //resolve input argument
  val (master: String, file: String, saveFile: String) = args.toList match {
    case firstArg :: secondArg :: thirdArg :: Nil => (firstArg, secondArg, thirdArg)
    case firstArg :: Nil => (firstArg, "/homework/spark/train.csv", "./" + new Date().getTime)
    case Nil => ("local", "/homework/spark/train.csv", "./" + new Date().getTime)
  }

  /**SparkContext init*/
  private val sparkContext: SparkContext = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .master(master)
    .getOrCreate()
    .sparkContext

  /**reading text file*/
  private val inputRDD: RDD[String] = sparkContext
    .textFile(file)
    .cache()

  /** created in Engine two methods (single, isHeaderCsv) to filter RDD[String] and split into RDD[Array[String]] by comma */
  private val noHeaderRDD: RDD[Array[String]] = inputRDD
    .filter(!isHeaderCsv(_))
    .map(e => e.split(","))
    .filter(arr => arr(16).toInt == arr(3).toInt)

  /**using predef in Engine param to get value of Hotel country */
  private val parsedRDD: RDD[(Int, Int)] = noHeaderRDD
    .map(line => (1, line(hotel_country).toInt))

  /** swapping key and value*/
  private val mapRDD = parsedRDD.map(_.swap)

  /** creating vals for aggregateByKey */
  private val initialCount = 0
  private val addToCounts = (n: Int, v: Int) => n + v
  private val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

  private val countByKey = mapRDD
    .aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    .sortBy(_._2, false)

  countByKey.take(1).foreach(println)

}
