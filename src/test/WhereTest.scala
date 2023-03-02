package org.virtuslab.iskra.test

class WhereTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*
  import functions.lit

  case class Foo(int: Int, intOpt: Option[Int])

  val foos = Seq(
    Foo(1, Some(1)),
    Foo(2, None),
    Foo(3, Some(3))
  ).toDF.asStruct

  test("where-nonnullable") {
    val result = foos
      .where($.int >= lit(2))
      .select($.intOpt)
      .asClass[Option[Int]].collect().toList

    result shouldEqual Seq(None, Some(3))
  }

  test("where-nullable") {
    val result = foos
      .where($.intOpt >= lit(2))
      .select($.int)
      .asClass[Int].collect().toList

    result shouldEqual Seq(3)
  }
