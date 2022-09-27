package org.virtuslab.iskra
package types

import DataType.{CommonNumericNonNullableType, CommonNumericNullableType, NumericOptType, NumericType}

trait Coerce[-A <: DataType, -B <: DataType]:
  type Coerced <: DataType

object Coerce:
  given sameType[A <: DataType]: Coerce[A, A] with
    override type Coerced = A

  given nullable[A <: NumericOptType, B <: NumericOptType]: Coerce[A, B] with
    override type Coerced = CommonNumericNullableType[A, B]

  given nonNullable[A <: NumericType, B <: NumericType]: Coerce[A, B] with
    override type Coerced = CommonNumericNonNullableType[A, B]
