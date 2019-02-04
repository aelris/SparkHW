package com.epam.spark3

/**
  * object with main logic
  * inner object sparkContext*/
trait Engine {

  val srch_adults_cnt = 13
  val is_booked = 18
  val hotel_continent = 20
  val hotel_country = 21
  val hotel_market = 22
  val  user_location_country = 3
  val srch_children_cnt = 14

  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time")
  def isBooked(line: String): Boolean  = line(is_booked).toInt > 0
  def withChild(line: String): Boolean = line(srch_children_cnt) > 0


}
