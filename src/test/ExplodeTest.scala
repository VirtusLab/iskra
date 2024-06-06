package org.virtuslab.iskra.test

class ExplodeTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*
  import functions.explode

  case class Foo(ints: Seq[Int])

  val foos = Seq(
    Foo(Seq(1)),
    Foo(Seq(2)),
    Foo(Seq()),
    Foo(null),
    Foo(Seq(3,4))
  ).toTypedDF

  test("explode") {
    val result = foos
      .select(explode($.ints).as("int"))
      .collectAs[Int]

    result shouldEqual Seq(1,2,3,4)
  }

