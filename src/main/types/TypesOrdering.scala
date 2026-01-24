package org.virtuslab.iskra.types

trait TypesOrdering[T1 <: DataType, T2 <: DataType]

object TypesOrdering:
  given orderingNumericTypes[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: TypesOrdering[T1, T2] = new TypesOrdering[T1, T2] {}
  given orderingBooleanTypes[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: TypesOrdering[T1, T2] = new TypesOrdering[T1, T2] {}
  given orderingStringTypes[T1 <: StringOptLike, T2 <: StringOptLike]: TypesOrdering[T1, T2] = new TypesOrdering[T1, T2] {}
