package com.epam.spark2

import org.junit.{Assert, Before, Test}
import org.mockito.{InjectMocks, Mock, MockitoAnnotations}


class MainTest {
  @Mock private val MODULE$ = null
  @InjectMocks private val main$ = Main

  @Before def setUp(): Unit = {
    MockitoAnnotations.initMocks(this)
  }

  @Test
  @throws[Exception]
  def testIsHeaderCsv(): Unit = {
    val result = main$.isHeaderCsv("date_time,site_name,posa_continent")
    Assert.assertEquals(true, result)
  }

}
