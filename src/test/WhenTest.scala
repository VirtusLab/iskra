package org.virtuslab.iskra.test

class WhenTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*
  import functions.{lit, when}
  import Column.=== // by default shadowed by === from scalatest

  case class Foo(int: Int)

  val foos = Seq(
    Foo(1),
    Foo(2),
    Foo(3)
  ).toTypedDF

  test("when-without-fallback") {
    val result = foos
      .select(when($.int === lit(1), lit("a")).as("strOpt"))
      .collectAs[Option[String]]

    result shouldEqual Seq(Some("a"), None, None)
  }

  test("when-with-fallback") {
    val result = foos
      .select{
        when($.int === lit(1), lit(10))
          .otherwise(lit(100d))
          .as("double")
      }
      .collectAs[Double]

    result shouldEqual Seq(10d, 100d, 100d)
  }

  test("when-else-when-without-fallback") {
    val result = foos
      .select{
        when($.int === lit(1), lit(10))
          .when($.int === lit(2), lit(100L))
          .as("longOpt")
      }
      .collectAs[Option[Long]]

    result shouldEqual Seq(Some(10L), Some(100L), None)
  }

  test("when-else-when-with-fallback") {
    val result = foos
      .select{
        when($.int === lit(1), lit(10))
          .when($.int === lit(2), lit(100L))
          .otherwise(lit(1000d))
          .as("str")
      }
      .collectAs[Option[Double]]

    result shouldEqual Seq(Some(10d), Some(100d), Some(1000d))
  }
