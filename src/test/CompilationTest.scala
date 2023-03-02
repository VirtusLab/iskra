package org.virtuslab.iskra.test

import org.scalatest.funsuite.AnyFunSuite

class CompilationTest extends AnyFunSuite:
  test("Select nonexistent column") {
    assertCompiles("""
      |import org.virtuslab.iskra.api.*
      |case class Foo(string: String)
      |given spark: SparkSession = ???
      |val elements = Seq(Foo("abc")).toDF.asStruct.select($.string)
      |""".stripMargin)

    assertDoesNotCompile("""
      |import org.virtuslab.iskra.api.*
      |case class Foo(string: String)
      |given spark: SparkSession = ???
      |val elements = Seq(Foo("abc")).toDF.asStruct.select($.strin)
      |""".stripMargin)
  }
