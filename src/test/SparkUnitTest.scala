package org.virtuslab.iskra.test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.virtuslab.iskra.api.*

abstract class SparkUnitTest extends AnyFunSuite, BeforeAndAfterAll:
  def appName: String = getClass.getSimpleName
  
  given spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(suiteName)
      .getOrCreate()
  
  override def afterAll() =
    spark.stop()

  export org.scalatest.matchers.should.Matchers.shouldEqual