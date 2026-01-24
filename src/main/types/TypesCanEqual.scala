package org.virtuslab.iskra.types

trait TypesCanEqual[T1 <: DataType, T2 <: DataType]

object TypesCanEqual:
  given canEqualNumericTypes[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: TypesCanEqual[T1, T2] = new TypesCanEqual[T1, T2] {}
  given canEqualBooleanTypes[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: TypesCanEqual[T1, T2] = new TypesCanEqual[T1, T2] {}
  given canEqualStringTypes[T1 <: StringOptLike, T2 <: StringOptLike]: TypesCanEqual[T1, T2] = new TypesCanEqual[T1, T2] {}
