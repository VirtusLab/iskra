package org.virtuslab.iskra
package test

import org.scalatest.funsuite.AnyFunSuite
import types.*

class CoerceTest extends AnyFunSuite:
  test("coerce-int-double") {
    val c = summon[Coerce[IntegerType, DoubleType]]
    summon[c.Coerced =:= DoubleType]
  }

  test("coerce-short-short-opt") {
    val c = summon[Coerce[ShortType, ShortOptType]]
    summon[c.Coerced =:= ShortOptType]
  }

  test("coerce-long-byte-opt") {
    val c = summon[Coerce[LongType, ByteOptType]]
    summon[c.Coerced =:= LongOptType]
  }

  test("coerce-string-string-opt") {
    val c = summon[Coerce[StringType, StringOptType]]
    summon[c.Coerced =:= StringOptType]
  }
