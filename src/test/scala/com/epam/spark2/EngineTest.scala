package com.epam.spark2

import org.junit.{Assert, Test};


class EngineTest {
  private  val engine = new Engine {}

  @Test
  @throws[Exception]
  def testIsSameCountry(): Unit = {
    val result = engine.isSameCountry("1,2,4,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23")
    Assert.assertEquals(false, result)
  }

  @Test
  @throws[Exception]
  def testIsHeaderCsv(): Unit = {
    val result = engine.isHeaderCsv("date_time,site_name,posa_continent")
    Assert.assertEquals(true, result)
  }

}