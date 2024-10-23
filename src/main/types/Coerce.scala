package org.virtuslab.iskra
package types

trait Coerce[A <: DataType, B <: DataType]:
  type Coerced <: DataType

object Coerce extends CoerceLowPrio:
  given sameType[A <: FinalDataType]: Coerce[A, A] with
    override type Coerced = A

  given nullableFirst[A <: FinalDataType & Nullable, B <: FinalDataType & NonNullable](using A <:< NullableOf[B]): Coerce[A, B] with
    override type Coerced = A

  given nullableSecond[A <: FinalDataType & NonNullable, B <: FinalDataType & Nullable](using A <:< NonNullableOf[B]): Coerce[A, B] with
    override type Coerced = B

trait CoerceLowPrio:
  given numeric[A <: FinalDataType & DoubleOptLike, B <: FinalDataType & DoubleOptLike]: (Coerce[A, B] { type Coerced = CommonNumericType[A, B] }) =
    new Coerce[A, B]:
      override type Coerced = CommonNumericType[A, B]
