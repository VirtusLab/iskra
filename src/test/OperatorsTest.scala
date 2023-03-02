package org.virtuslab.iskra.test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers.shouldEqual

class OperatorsTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*

  case class Foo(boolean: Boolean, string: String, byte: Byte, short: Short, int: Int, long: Long, float: Float, double: Double)
  case class Bar(int: Int, intSome: Option[Int], intNone: Option[Int])

  val foos = Seq(
    Foo(true, "abc", 1, 2, 3, 4, 5.0, 6.0)
  ).toDF.asStruct

  val bars = Seq(
    Bar(1, Some(10), None),
  ).toDF.asStruct

  test("plus") {
    val result = foos.select(
      ($.byte + $.byte).as("_1"),
      ($.short + $.short).as("_2"),
      ($.int + $.int).as("_3"),
      ($.long + $.long).as("_4"),
      ($.float + $.float).as("_5"),
      ($.double + $.double).as("_6"),
      ($.short + $.float).as("_7"),
    ).asClass[(Byte, Short, Int, Long, Float, Double, Float)].collect().toList

    result shouldEqual List((2, 4, 6, 8, 10.0, 12.0, 7.0))
  }

  test("minus") {
    val result = foos.select(
      ($.byte - $.byte).as("_1"),
      ($.short - $.short).as("_2"),
      ($.int - $.int).as("_3"),
      ($.long - $.long).as("_4"),
      ($.float - $.float).as("_5"),
      ($.double - $.double).as("_6"),
      ($.double - $.byte).as("_7")
    ).asClass[(Byte, Short, Int, Long, Float, Double, Double)].collect().toList

    result shouldEqual List((0, 0, 0, 0, 0.0, 0.0, 5.0))
  }

  test("mult") {
    val result = foos.select(
      ($.byte * $.byte).as("_1"),
      ($.short * $.short).as("_2"),
      ($.int * $.int).as("_3"),
      ($.long * $.long).as("_4"),
      ($.float * $.float).as("_5"),
      ($.double * $.double).as("_6"),
      ($.int * $.float).as("_7"),
    ).asClass[(Byte, Short, Int, Long, Float, Double, Float)].collect().toList

    result shouldEqual List((1, 4, 9, 16, 25.0, 36.0, 15.0))
  }

  test("div") {
    val result = foos.select(
      ($.byte / $.byte).as("_1"),
      ($.short / $.short).as("_2"),
      ($.int / $.int).as("_3"),
      ($.long / $.long).as("_4"),
      ($.float / $.float).as("_5"),
      ($.double / $.double).as("_6"),
      ($.long / $.short).as("_7"),
    ).asClass[(Double, Double, Double, Double, Double, Double, Double)].collect().toList

    result shouldEqual List((1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0))
  }

  test("plusplus") {
    val result = foos.select(
      ($.string ++ $.string).as("_1"),
    ).asClass[String].collect().toList

    result shouldEqual List("abcabc")
  }

  test("eq") {
    import Column.=== // by default shadowed by === from scalatest

    val result = foos.select(
      ($.boolean === $.boolean).as("_1"),
      ($.string === $.string).as("_2"),
      ($.byte === $.byte).as("_3"),
      ($.short === $.short).as("_4"),
      ($.int === $.int).as("_5"),
      ($.long === $.long).as("_6"),
      ($.float === $.float).as("_7"),
      ($.double === $.double).as("_8"),
      ($.byte === $.int).as("_9"),
    ).asClass[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)].collect().toList

    result shouldEqual List((true, true, true, true, true, true, true, true, false))
  }

  test("ne") {
    val result = foos.select(
      ($.boolean =!= $.boolean).as("_1"),
      ($.string =!= $.string).as("_2"),
      ($.byte =!= $.byte).as("_3"),
      ($.short =!= $.short).as("_4"),
      ($.int =!= $.int).as("_5"),
      ($.long =!= $.long).as("_6"),
      ($.float =!= $.float).as("_7"),
      ($.double =!= $.double).as("_8"),
      ($.short =!= $.float).as("_9"),
    ).asClass[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)].collect().toList

    result shouldEqual List((false, false, false, false, false, false, false, false, true))
  }

  test("lt") {
    val result = foos.select(
      ($.boolean < $.boolean).as("_1"),
      ($.string < $.string).as("_2"),
      ($.byte < $.byte).as("_3"),
      ($.short < $.short).as("_4"),
      ($.int < $.int).as("_5"),
      ($.long < $.long).as("_6"),
      ($.float < $.float).as("_7"),
      ($.double < $.double).as("_8"),
      ($.byte < $.double).as("_9"),
    ).asClass[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)].collect().toList

    result shouldEqual List((false, false, false, false, false, false, false, false, true))
  }

  test("le") {
    val result = foos.select(
      ($.boolean <= $.boolean).as("_1"),
      ($.string <= $.string).as("_2"),
      ($.byte <= $.byte).as("_3"),
      ($.short <= $.short).as("_4"),
      ($.int <= $.int).as("_5"),
      ($.long <= $.long).as("_6"),
      ($.float <= $.float).as("_7"),
      ($.double <= $.double).as("_8"),
      ($.long <= $.byte).as("_9"),
    ).asClass[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)].collect().toList

    result shouldEqual List((true, true, true, true, true, true, true, true, false))
  }

  test("gt") {
    val result = foos.select(
      ($.boolean > $.boolean).as("_1"),
      ($.string > $.string).as("_2"),
      ($.byte > $.byte).as("_3"),
      ($.short > $.short).as("_4"),
      ($.int > $.int).as("_5"),
      ($.long > $.long).as("_6"),
      ($.float > $.float).as("_7"),
      ($.double > $.double).as("_8"),
      ($.float > $.long).as("_9"),
    ).asClass[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)].collect().toList

    result shouldEqual List((false, false, false, false, false, false, false, false, true))
  }

  test("ge") {
    val result = foos.select(
      ($.boolean >= $.boolean).as("_1"),
      ($.string >= $.string).as("_2"),
      ($.byte >= $.byte).as("_3"),
      ($.short >= $.short).as("_4"),
      ($.int >= $.int).as("_5"),
      ($.long >= $.long).as("_6"),
      ($.float >= $.float).as("_7"),
      ($.double >= $.double).as("_8"),
      ($.short >= $.int).as("_9"),
    ).asClass[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)].collect().toList

    result shouldEqual List((true, true, true, true, true, true, true, true, false))
  }

  test("and") {
    val result = foos.select(
      ($.boolean && $.boolean).as("_1"),
    ).asClass[Boolean].collect().toList

    result shouldEqual List(true)
  }

  test("or") {
    val result = foos.select(
      ($.boolean || $.boolean).as("_1"),
    ).asClass[Boolean].collect().toList

    result shouldEqual List(true)
  }

  test("plus nullable") {
    val result = bars.select(
      ($.int + $.intSome).as("_1"),
      ($.int + $.intNone).as("_2"),
      ($.intSome + $.int).as("_3"),
      ($.intNone + $.int).as("_4"),
      ($.intSome + $.intSome).as("_5"),
      ($.intSome + $.intNone).as("_6"),
      ($.intNone + $.intSome).as("_7"),
      ($.intNone + $.intNone).as("_8"),
    ).asClass[(Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int])].collect().toList

    result shouldEqual List((Some(11), None, Some(11), None, Some(20), None, None, None))
  }
