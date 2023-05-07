package org.virtuslab.iskra.test

class WithColumnsTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*

  case class Foo(a: Int, b: Int)
  case class Bar(a: Int, b: Int, c: Int)
  case class Baz(a: Int, b: Int, c: Int, d: Int)

  val foos = Seq(
    Foo(1, 2)
  ).toDF.asStruct

  test("withColumns-single") {
    val result = foos
      .withColumns(
        ($.a + $.b).as("c")
      )
      .asClass[Bar].collect().toList

    result shouldEqual List(Bar(1, 2, 3))
  }

  test("withColumns-many") {
    val result = foos
      .withColumns(
        ($.a + $.b).as("c"),
        ($.a - $.b).as("d"),
      )
      .asClass[Baz].collect().toList

    result shouldEqual List(Baz(1, 2, 3, -1))
  }
