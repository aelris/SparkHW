package com.epam.spark1

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * start point*/
object Main extends App with Engine {

  println(args.mkString("\n"))

  /**resolve input argument*/
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
    .getOrCreate().sparkContext

  /**reading text file*/
  private val inputRDD: RDD[String] = sparkContext.textFile(file)

  /** created in Engine two methods (single, isHeaderCsv) to filter RDD[String] and split into RDD[Array[String]] by comma */
  private val noHeaderCouplesRDD: RDD[Array[String]] = inputRDD.filter(!isHeaderCsv(_)).filter(!single(_))
    .map(_.split(","))

  /** columns: is_booked = 18, hotel_continent = 20, val hotel_country = 21, val hotel_market = 22*/
  private val parsedRDD: RDD[(Int, (Int, Int, Int))] = noHeaderCouplesRDD.map(line =>{
    (1, (line(hotel_continent).toInt, line(hotel_country).toInt, line(hotel_market).toInt))})

  /** swapping key and values from Int,(Int,Int,Int) to (Int,Int,Int),Int*/
  private val mapRDD = parsedRDD.map(_.swap)

  /** creating vals for aggregateByKey */
  private val initialCount = 0
  private val addToCounts = (n: Int, v: Int) => n+v
  private val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2


  private val countByKey = mapRDD.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    .sortBy(_._2,false)
  countByKey.take(3).foreach(println)

}
