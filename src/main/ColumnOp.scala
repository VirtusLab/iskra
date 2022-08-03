package org.virtuslab.typedframes

import scala.quoted.*
import org.apache.spark.sql
import org.apache.spark.sql.functions.concat
import org.virtuslab.typedframes.{Column as Col}
import org.virtuslab.typedframes.UntypedOps.typed
import org.virtuslab.typedframes.types.*
import DataType.*

object ColumnOp:
  trait Plus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped + col2.untyped).typed[Out]
  object Plus:
    transparent inline given numeric[T1 <: NumericOptType, T2 <: NumericOptType]: Plus[T1, T2] = ${ numericImpl[T1, T2] }
    private def numericImpl[T1 <: NumericOptType : Type, T2  <: NumericOptType : Type](using Quotes) =
      DataType.commonNumericType[T1, T2] match
        case '[t] =>
          '{
            (new Plus[T1, T2] { type Out = t }): Plus[T1, T2] { type Out = t }
          }

  trait Minus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped - col2.untyped).typed[Out]
  object Minus:
    transparent inline given numeric[T1 <: NumericOptType, T2 <: NumericOptType]: Minus[T1, T2] = ${ numericImpl[T1, T2] }
    private def numericImpl[T1 <: NumericOptType : Type, T2  <: NumericOptType : Type](using Quotes) =
      DataType.commonNumericType[T1, T2] match
        case '[t] =>
          '{
            (new Minus[T1, T2] { type Out = t }): Minus[T1, T2] { type Out = t }
          }

  trait Mult[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped * col2.untyped).typed[Out]
  object Mult:
    transparent inline given numeric[T1 <: NumericOptType, T2 <: NumericOptType]: Mult[T1, T2] = ${ numericImpl[T1, T2] }
    private def numericImpl[T1 <: NumericOptType : Type, T2  <: NumericOptType : Type](using Quotes) =
      DataType.commonNumericType[T1, T2] match
        case '[t] =>
          '{
            (new Mult[T1, T2] { type Out = t }): Mult[T1, T2] { type Out = t }
          }

  trait Div[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped / col2.untyped).typed[Out]
  object Div:
    transparent inline given numeric[T1 <: NumericOptType, T2 <: NumericOptType]: Div[T1, T2] = ${ numericImpl[T1, T2] }
    private def numericImpl[T1 <: NumericOptType : Type, T2  <: NumericOptType : Type](using Quotes) =
      val outType = Type.of[(T1, T2)] match
        case '[(NotNull, NotNull)] => Type.of[DoubleType]
        case _ => Type.of[DoubleOptType]
      outType match
        case '[t] =>
          '{
            (new Div[T1, T2] { type Out = t }): Div[T1, T2] { type Out = t }
          }

  trait PlusPlus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = concat(col1.untyped, col2.untyped).typed[Out]
  object PlusPlus:
    transparent inline given string[T1 <: StringOptType, T2 <: StringOptType]: PlusPlus[T1, T2] = ${ stringImpl[T1, T2] }
    private def stringImpl[T1 <: StringOptType : Type, T2  <: StringOptType : Type](using Quotes) =
      val outType = Type.of[(T1, T2)] match
        case '[(NotNull, NotNull)] => Type.of[StringType]
        case _ => Type.of[StringOptType]
      outType match
        case '[t] =>
          '{
            (new PlusPlus[T1, T2] { type Out = t }): PlusPlus[T1, T2] { type Out = t }
          }

  // TODO: Make equality multiversal but allow to compare numeric types
  trait Eq[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped === col2.untyped).typed[Out]
  object Eq:
    given nonNullable[T <: NotNull]: Eq[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Eq[T, T] with
      type Out = BooleanOptType

  trait Ne[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped =!= col2.untyped).typed[Out]
  object Ne:
    given nonNullable[T <: NotNull]: Ne[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Ne[T, T] with
      type Out = BooleanOptType

  trait Lt[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped < col2.untyped).typed[Out]
  object Lt:
    given nonNullable[T <: NotNull]: Lt[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Lt[T, T] with
      type Out = BooleanOptType

  trait Le[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped <= col2.untyped).typed[Out]
  object Le:
    given nonNullable[T <: NotNull]: Le[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Le[T, T] with
      type Out = BooleanOptType

  trait Gt[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped > col2.untyped).typed[Out]
  object Gt:
    given nonNullable[T <: NotNull]: Gt[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Gt[T, T] with
      type Out = BooleanOptType

  trait Ge[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped >= col2.untyped).typed[Out]
  object Ge:
    given nonNullable[T <: NotNull]: Ge[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Ge[T, T] with
      type Out = BooleanOptType

  trait And[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped && col2.untyped).typed[Out]
  object And:
    given nonNullable[T <: NotNull]: And[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: And[T, T] with
      type Out = BooleanOptType

  trait Or[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped || col2.untyped).typed[Out]
  object Or:
    given nonNullable[T <: NotNull]: Or[T, T] with
      type Out = BooleanType
    given nullable[T <: DataType]: Or[T, T] with
      type Out = BooleanOptType
