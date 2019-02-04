package com.epam.spark2

/**
  * object with main logic
  * inner object sparkContext*/
trait Engine {

  val srch_adults_cnt = 13
  val is_booked = 18
  val hotel_continent = 20
  val hotel_country = 21
  val hotel_market = 22
  val user_location_country = 16

  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time")
  def isSameCountry(line: String): Boolean = line(hotel_country) == line(user_location_country)


}
