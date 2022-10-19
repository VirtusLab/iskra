package org.virtuslab.iskra
package test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

// import org.virtuslab.iskra._

abstract class SparkUnitTest extends AnyFunSuite with BeforeAndAfterAll {
  def appName: String = getClass.getSimpleName
  
  implicit lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(suiteName)
      .getOrCreate()
  
  override def afterAll() =
    spark.stop()

  // export org.scalatest.matchers.should.Matchers.shouldEqual
}