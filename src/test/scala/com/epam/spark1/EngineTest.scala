package scala.com.epam.spark

import com.epam.spark1.Engine
import org.junit.{Assert, Test}

class EngineTest {
  @Test
  def testIsHeaderCsv(): Unit = {
    val result = new Engine() {}.isHeaderCsv("date_time,site_name,posa_continent")
    Assert.assertEquals(true, result)
  }

  @Test
    def testSingle(): Unit = {
    val result = new Engine() {}.single("0,1,1,0,1,0,1,1,0,1,1,1,2")
    Assert.assertEquals(false, result)
  }
//  val testPath: String = "src/test/resources/train.csv"
//  @Test
//  def IsRddCorrect(): Unit = {
//
//    val rdd = new Engine(){}.createRDD(testPath)
//
//    Assert.assertNotNull(rdd)
//
//  }
//
//  @Test
//  def createHotelsRDDTest(): Unit = {
//
//    val expected: Int = 874
//
//    val rdd = new Engine(){}.createHotelsRDD(testPath)
//
//    val actual: Long = rdd.count()
//
//
//
//    Assert.assertEquals(expected, actual)
//
//  }
}
