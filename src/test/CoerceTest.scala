package org.virtuslab.iskra
package test

import org.scalatest.funsuite.AnyFunSuite
import types.*

class CoerceTest extends AnyFunSuite:
  test("coerce-int-double") {
    val c = summon[Coerce[IntNotNull, DoubleNotNull]]
    summon[c.Coerced =:= DoubleNotNull]
  }

  test("coerce-short-short-opt") {
    val c = summon[Coerce[ShortNotNull, ShortOrNull]]
    summon[c.Coerced =:= ShortOrNull]
  }

  test("coerce-long-byte-opt") {
    val c = summon[Coerce[LongNotNull, ByteOrNull]]
    summon[c.Coerced =:= LongOrNull]
  }

  test("coerce-string-string-opt") {
    val c = summon[Coerce[StringNotNull, StringOrNull]]
    summon[c.Coerced =:= StringOrNull]
  }

  test("coerce-string-opt-string") {
    val c = summon[Coerce[StringOrNull, StringNotNull]]
    summon[c.Coerced =:= StringOrNull]
  }

  test("coerce-string-opt-string-opt") {
    val c = summon[Coerce[StringOrNull, StringOrNull]]
    summon[c.Coerced =:= StringOrNull]
  }
