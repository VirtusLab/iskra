package org.virtuslab.iskra.test

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers.shouldEqual

class OperatorsErrorsTest extends SparkUnitTest:
  import scala.compiletime.testing.typeCheckErrors

  test("columns addition args error") {

    val errors = typeCheckErrors("""
      |{
      |import org.virtuslab.iskra.api.*
      |val ci: Col[int] = ???
      |val cb: Col[boolean] = ???
      |val cp = ci + cb
      |cp
      |}
    """.stripMargin)

    val expectedErrorMessages = List(
      "Cannot apply operator + to columns of types int and boolean: expected 2 numeric columns"
    )

    errors.map(_.message) shouldEqual expectedErrorMessages
  }

  test("columns equality args error") {

    val errors = typeCheckErrors("""
      |{
      |import org.virtuslab.iskra.api.*
      |val ci: Col[int] = ???
      |val cb: Col[boolean] = ???
      |val cp = ci === cb
      |cp
      |}
    """.stripMargin)

    val expectedErrorMessages = List(
      "Cannot apply operator === to columns of types int and boolean: Implementation of TypesCanEqual for these types is missing"
    )

    errors.map(_.message) shouldEqual expectedErrorMessages
  }

  test("columns comparison args error") {

    val errors = typeCheckErrors("""
      |{
      |import org.virtuslab.iskra.api.*
      |val ci: Col[int] = ???
      |val cb: Col[boolean] = ???
      |val cp = ci < cb
      |cp
      |}
    """.stripMargin)

    val expectedErrorMessages = List(
      "Cannot apply operator < to columns of types int and boolean: Implementation of TypesOrdering for these types is missing"
    )

    errors.map(_.message) shouldEqual expectedErrorMessages
  }
