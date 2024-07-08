package org.virtuslab.iskra.test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers.shouldEqual

class ColumnsTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*

  case class Foo(x1: Int, x2: Int, x3: Int, x4: Int)

  val foos = Seq(
    Foo(1, 2, 3, 4)
  ).toDF.asStruct

  test("plus") {
    val result = foos.select {
      val cols1 = Columns($.x1)
      val cols2 = Columns($.x2, $.x3)
      (cols1, cols2, $.x4)
    }.asClass[Foo].collect().toList

    result shouldEqual List(Foo(1, 2, 3, 4))
  }
