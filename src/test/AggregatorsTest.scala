package org.virtuslab.iskra.test

class AggregatorsTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*
  import functions.*

  case class Foo(string: String, int: Int, intOpt: Option[Int], float: Float, floatOpt: Option[Float])

  val foos = Seq(
    Foo("a", 1, Some(1), 1.0f, Some(1.0f)),
    Foo("a", 3, None, 3.0f, None)
  ).toTypedDF

  test("sum") {
    val result = foos.groupBy($.string).agg(
      sum($.int).as("_1"),
      sum($.intOpt).as("_2"),
      sum($.float).as("_3"),
      sum($.floatOpt).as("_4"),
    )
    .select($._1, $._2, $._3, $._4)
    .collectAs[(Option[Int], Option[Int], Option[Float], Option[Float])]

    result shouldEqual Seq((Some(4), Some(1), Some(4.0f), Some(1.0f)))
  }

  test("max") {
    val result = foos.groupBy($.string).agg(
      max($.int).as("_1"),
      max($.intOpt).as("_2"),
      max($.float).as("_3"),
      max($.floatOpt).as("_4"),
    )
    .select($._1, $._2, $._3, $._4)
    .collectAs[(Option[Int], Option[Int], Option[Float], Option[Float])]

    result shouldEqual Seq((Some(3), Some(1), Some(3.0f), Some(1.0f)))
  }

  test("min") {
    val result = foos.groupBy($.string).agg(
      min($.int).as("_1"),
      min($.intOpt).as("_2"),
      min($.float).as("_3"),
      min($.floatOpt).as("_4"),
    )
    .select($._1, $._2, $._3, $._4)
    .collectAs[(Option[Int], Option[Int], Option[Float], Option[Float])]

    result shouldEqual Seq((Some(1), Some(1), Some(1.0f), Some(1.0f)))
  }

  test("avg") {
    val result = foos.groupBy($.string).agg(
      avg($.int).as("_1"),
      avg($.intOpt).as("_2"),
      avg($.float).as("_3"),
      avg($.floatOpt).as("_4"),
    )
    .select($._1, $._2, $._3, $._4)
    .collectAs[(Option[Double], Option[Double], Option[Double], Option[Double])]

    result shouldEqual Seq((Some(2.0), Some(1.0), Some(2.0), Some(1.0)))
  }