package com.epam.spark3

import org.junit.{Assert, Before, Test}
import org.mockito.{InjectMocks, Mock, MockitoAnnotations}


class Main$Test {
  @Mock private[spark3] val MODULE$ = null
  @InjectMocks private[spark3] val main$ = Main

  @Before def setUp(): Unit = {
    MockitoAnnotations.initMocks(this)
  }

  @Test
  @throws[Exception]
  def testWithChild(): Unit = {
    val result = main$.withChild("1,2,4,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23")
    Assert.assertEquals(false, result)
  }

  @Test
  @throws[Exception]
  def testIsHeaderCsv(): Unit = {
    val result = main$.isHeaderCsv("1,2,4,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23")
    Assert.assertEquals(false, result)
  }

  @Test
  @throws[Exception]
  def testIsBooked(): Unit = {
    val result = main$.isBooked("1,2,4,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23")
    Assert.assertEquals(false, result)
  }

}