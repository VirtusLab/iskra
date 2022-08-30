package org.virtuslab.typedframes.test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.virtuslab.typedframes.api.*

abstract class SparkUnitTest(suiteName: String) extends AnyFunSuite, BeforeAndAfterAll:
  given spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(suiteName)
      .getOrCreate()
  
  override def afterAll() =
    spark.stop()

  export org.scalatest.matchers.should.Matchers.shouldEqual