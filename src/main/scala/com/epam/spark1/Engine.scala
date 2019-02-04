package com.epam.spark1


/**
  * object with main logic
  * inner object sparkContext*/
trait Engine {

  //Variables for indexes in RDD[Array[String]]
  val srch_adults_cnt = 13
  val is_booked = 18
  val hotel_continent = 20
  val hotel_country = 21
  val hotel_market = 22

  //Two methods for 1 task
  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time")
  def single(line: String): Boolean  = line(srch_adults_cnt).toInt < 2


}
