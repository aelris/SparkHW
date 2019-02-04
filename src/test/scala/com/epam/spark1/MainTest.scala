package com.epam.spark1

import org.junit.{Assert, Before, Test}
import org.mockito.{InjectMocks, MockitoAnnotations}


class MainTest {

  @InjectMocks private[spark1] val main$ = Main

  @Before def setUp(): Unit = {
    MockitoAnnotations.initMocks(this)
  }

  @Test
  @throws[Exception]
  def testIsHeaderCsv(): Unit = {
    val result = main$.isHeaderCsv("date_time,site_name,posa_continent")
    Assert.assertEquals(true, result)
  }

  @Test
  @throws[Exception]
  def testSingle(): Unit = {
    val result = main$.single("3,7,3,1,6,8,3,5,2,6,8,9,3,5,7,8,3,5")
    Assert.assertEquals(false, result)
  }
}
