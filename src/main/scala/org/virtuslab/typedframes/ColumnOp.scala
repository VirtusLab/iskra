package org.virtuslab.typedframes

import org.apache.spark.sql
import org.virtuslab.typedframes.{TypedColumn as Col}
import org.virtuslab.typedframes.TypedColumn.untypedColumnOps
import org.virtuslab.typedframes.types.*

// trait ColumnOp

object ColumnOp:
  type AdditiveResult[T1 <: DataType, T2 <: DataType] <: DataType = (T1, T2) match
    case (IntegerType, IntegerType) => IntegerType
    case (IntegerType, FloatType) => FloatType
    case (FloatType, IntegerType) => FloatType
    case (FloatType, FloatType) => FloatType
    case (IntegerType, DoubleType) => DoubleType
    case (DoubleType, IntegerType) => DoubleType
    case (FloatType, DoubleType) => DoubleType
    case (DoubleType, FloatType) => DoubleType
    case (DoubleType, DoubleType) => DoubleType

  trait Plus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped + col2.untyped).typed[Out]
  object Plus:
    given [T1 <: DataType, T2 <: DataType]: Plus[T1, T2] with
      type Out = AdditiveResult[T1, T2]
    // given Plus[IntegerType, IntegerType] with
    //   type Out = Col[IntegerType]
    // given Plus[IntegerType, DoubleType] with
    //   type Out = DoubleType
    // given Plus[DoubleType, IntegerType] with
    //   type Out = DoubleType
    // given Plus[DoubleType, DoubleType] with
    //   type Out = DoubleType

  trait Minus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped - col2.untyped).typed[Out]
  object Minus:
    given [T1 <: DataType, T2 <: DataType]: Minus[T1, T2] with
      type Out = AdditiveResult[T1, T2]

  trait PlusPlus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out]
  object PlusPlus:
    given PlusPlus[StringType, StringType] with
      type Out = StringType
      def apply(col1: Col[StringType], col2: Col[StringType]): Col[Out] = (sql.functions.concat(col1.untyped, col2.untyped)).typed[Out]

  trait Eq[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped === col2.untyped).typed[Out]
  object Eq:
    given Eq[BooleanType, BooleanType] with {}
    given Eq[StringType, StringType] with {}
    given Eq[IntegerType, IntegerType] with {}
    given Eq[DoubleType, DoubleType] with {}

  trait Ne[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped =!= col2.untyped).typed[Out]
  object Ne:
    given Ne[BooleanType, BooleanType] with {}
    given Ne[StringType, StringType] with {}
    given Ne[IntegerType, IntegerType] with {}
    given Ne[DoubleType, DoubleType] with {}

  trait Lt[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped < col2.untyped).typed[Out]
  object Lt:
    // TODO: Compare different numeric types?
    given Lt[StringType, StringType] with {}
    given Lt[IntegerType, IntegerType] with {}
    given Lt[FloatType, FloatType] with {}
    given Lt[DoubleType, DoubleType] with {}

  trait Le[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped < col2.untyped).typed[Out]
  object Le:
    given Le[StringType, StringType] with {}
    given Le[IntegerType, IntegerType] with {}
    given Le[FloatType, FloatType] with {}
    given Le[DoubleType, DoubleType] with {}

  trait Gt[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped < col2.untyped).typed[Out]
  object Gt:
    given Gt[StringType, StringType] with {}
    given Gt[IntegerType, IntegerType] with {}
    given Gt[FloatType, FloatType] with {}
    given Gt[DoubleType, DoubleType] with {}

  trait Ge[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped < col2.untyped).typed[Out]
  object Ge:
    given Ge[StringType, StringType] with {}
    given Ge[IntegerType, IntegerType] with {}
    given Ge[FloatType, FloatType] with {}
    given Ge[DoubleType, DoubleType] with {}

  trait And[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped && col2.untyped).typed[Out]
  object And:
    given And[BooleanType, BooleanType] with {}

  trait Or[T1 <: DataType, T2 <: DataType]:
    type Out = BooleanType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped || col2.untyped).typed[Out]
  object Or:
    given Or[BooleanType, BooleanType] with {}
