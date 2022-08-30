package org.virtuslab.typedframes.test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers.shouldEqual

class WhereTest extends SparkUnitTest("WhereTest"):
  import org.virtuslab.typedframes.api.*
  import functions.lit

  case class Foo(int: Int, intOpt: Option[Int])

  val foos = Seq(
    Foo(1, Some(1)),
    Foo(2, None),
    Foo(3, Some(3))
  ).toTypedDF

  test("where-nonnullable") {
    val result = foos
      .where($.int >= lit(2))
      .select($.intOpt)
      .collectAs[Option[Int]]

    result shouldEqual Seq(None, Some(3))
  }

  test("where-nullable") {
    val result = foos
      .where($.intOpt >= lit(2))
      .select($.int)
      .collectAs[Int]

    result shouldEqual Seq(3)
  }
