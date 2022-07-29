package org.virtuslab.example

import org.virtuslab.typedframes.api.*

object TypesTest:
  case class Foo(boolean: Boolean, string: String, byte: Byte, short: Short, int: Int, long: Long, float: Float, double: Double)
  case class Bar(int: Int, intOpt: Option[Int])
  case class Quux(aaa: Int, bbb: String)
  case class Quuz(quux: Quux)

  case class JustInt(i: Int)
  case class JustIntOpt(i: Option[Int])

  def main(args: Array[String]): Unit =
    given spark: SparkSession =
      SparkSession
        .builder()
        .master("local")
        .appName("Types test")
        .getOrCreate()

    val ints = Seq(1).toTypedDF
    val intOpts = Seq(Some(1), None).toTypedDF

    println(Seq(Option(JustInt(1))).toTypedDF.collectAs[Option[JustInt]])

    println(ints.collectAs[Int])
    println(ints.collectAs[Option[Int]])
    // println(intOpts.collectAs[Int]) // should not compile
    println(intOpts.collectAs[Option[Int]])

    val foos = Seq(
      Foo(true, "abc", 1, 2, 3, 4, 5.0, 6.0)
    ).toTypedDF

    val bars = Seq(
      Bar(1, Some(10)),
      Bar(2, None)
    ).toTypedDF

    println(
      foos.select(
        ($.byte + $.byte).as("_1"),
        ($.short + $.short).as("_2"),
        ($.int + $.int).as("_3"),
        ($.long + $.long).as("_4"),
        ($.float + $.float).as("_5"),
        ($.double + $.double).as("_6"),
        ($.short + $.float).as("_7"),
      ).collectAs[(Byte, Short, Int, Long, Float, Double, Float)]
    )

    println(
      foos.select(
        ($.byte - $.byte).as("_1"),
        ($.short - $.short).as("_2"),
        ($.int - $.int).as("_3"),
        ($.long - $.long).as("_4"),
        ($.float - $.float).as("_5"),
        ($.double - $.double).as("_6")
      ).collectAs[(Byte, Short, Int, Long, Float, Double)]
    )

    println(
      foos.select(
        ($.byte * $.byte).as("_1"),
        ($.short * $.short).as("_2"),
        ($.int * $.int).as("_3"),
        ($.long * $.long).as("_4"),
        ($.float * $.float).as("_5"),
        ($.double * $.double).as("_6")
      ).collectAs[(Byte, Short, Int, Long, Float, Double)]
    )

    println(
      foos.select(
        ($.byte / $.byte).as("_1"),
        ($.short / $.short).as("_2"),
        ($.int / $.int).as("_3"),
        ($.long / $.long).as("_4"),
        ($.float / $.float).as("_5"),
        ($.double / $.double).as("_6")
      ).collectAs[(Double, Double, Double, Double, Double, Double)]
    )

    println(
      foos.select(
        ($.byte / $.byte).as("_1"),
        ($.short / $.short).as("_2"),
        ($.int / $.int).as("_3"),
        ($.long / $.long).as("_4"),
        ($.float / $.float).as("_5"),
        ($.double / $.double).as("_6")
      ).collectAs[(Double, Double, Double, Double, Double, Double)]
    )

    println(
      foos.select(
        ($.string ++ $.string).as("_1"),
      ).collectAs[String]
    )

    println(
      foos.select(
        ($.boolean === $.boolean).as("_1"),
        ($.string === $.string).as("_2"),
        ($.byte === $.byte).as("_3"),
        ($.short === $.short).as("_4"),
        ($.int === $.int).as("_5"),
        ($.long === $.long).as("_6"),
        ($.float === $.float).as("_7"),
        ($.double === $.double).as("_8")
      ).collectAs[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]
    )

    println(
      foos.select(
        ($.boolean =!= $.boolean).as("_1"),
        ($.string =!= $.string).as("_2"),
        ($.byte =!= $.byte).as("_3"),
        ($.short =!= $.short).as("_4"),
        ($.int =!= $.int).as("_5"),
        ($.long =!= $.long).as("_6"),
        ($.float =!= $.float).as("_7"),
        ($.double =!= $.double).as("_8")
      ).collectAs[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]
    )

    println(
      foos.select(
        ($.boolean < $.boolean).as("_1"),
        ($.string < $.string).as("_2"),
        ($.byte < $.byte).as("_3"),
        ($.short < $.short).as("_4"),
        ($.int < $.int).as("_5"),
        ($.long < $.long).as("_6"),
        ($.float < $.float).as("_7"),
        ($.double < $.double).as("_8")
      ).collectAs[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]
    )

    println(
      foos.select(
        ($.boolean <= $.boolean).as("_1"),
        ($.string <= $.string).as("_2"),
        ($.byte <= $.byte).as("_3"),
        ($.short <= $.short).as("_4"),
        ($.int <= $.int).as("_5"),
        ($.long <= $.long).as("_6"),
        ($.float <= $.float).as("_7"),
        ($.double <= $.double).as("_8")
      ).collectAs[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]
    )

    println(
      foos.select(
        ($.boolean > $.boolean).as("_1"),
        ($.string > $.string).as("_2"),
        ($.byte > $.byte).as("_3"),
        ($.short > $.short).as("_4"),
        ($.int > $.int).as("_5"),
        ($.long > $.long).as("_6"),
        ($.float > $.float).as("_7"),
        ($.double > $.double).as("_8")
      ).collectAs[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]
    )

    println(
      foos.select(
        ($.boolean && $.boolean).as("_1"),
      ).collectAs[Boolean]
    )

    println(
      foos.select(
        ($.boolean || $.boolean).as("_1"),
      ).collectAs[Boolean]
    )

    println(
      bars.select(
        ($.int + $.int).as("_1"),
      ).collectAs[Int]
    )

    println(
      bars.select(
        ($.int + $.int).as("_1"),
      ).collectAs[Option[Int]]
    )

    // println(
    //   bars.select(
    //     ($.int + $.intOpt).as("_1"),
    //   ).collectAs[Int] // should not compile
    // )

    println(
      bars.select(
        ($.int + $.intOpt).as("_1"),
      ).collectAs[Option[Int]]
    )

    // println(
    //   bars.select(
    //     ($.intOpt + $.intOpt).as("_1"),
    //   ).collectAs[Int] // should not compile
    // )

    println(
      bars.select(
        ($.intOpt + $.intOpt).as("_1"),
      ).collectAs[Option[Int]]
    )

    println(
      bars.select(
        ($.int === $.int).as("_1"),
      ).collectAs[Boolean]
    )

    println(
      bars.select(
        ($.intOpt === $.int).as("_1"),
      ).collectAs[Option[Boolean]]
    )

    // println(
    //   bars.select(
    //     ($.intOpt === $.int).as("_1"),
    //   ).collectAs[Boolean] // should not compile
    // )

    spark.stop()
