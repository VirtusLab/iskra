package org.virtuslab.iskra

import scala.quoted.*
import org.apache.spark.sql
import org.apache.spark.sql.functions.concat
import org.virtuslab.iskra.Col
import org.virtuslab.iskra.UntypedOps.typed
import org.virtuslab.iskra.types.*
import DataType.*

object ColumnOp:
  trait Plus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped + col2.untyped).typed[Out]
  object Plus:
    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Plus[T1, T2] with
      type Out = DataType.CommonNumericNonNullableType[T1, T2]
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Plus[T1, T2] with
      type Out = DataType.CommonNumericNullableType[T1, T2]

  trait Minus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped - col2.untyped).typed[Out]
  object Minus:
    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Minus[T1, T2] with
      type Out = DataType.CommonNumericNonNullableType[T1, T2]
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Minus[T1, T2] with
      type Out = DataType.CommonNumericNullableType[T1, T2]

  trait Mult[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped * col2.untyped).typed[Out]
  object Mult:
    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Mult[T1, T2] with
      type Out = DataType.CommonNumericNonNullableType[T1, T2]
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Mult[T1, T2] with
      type Out = DataType.CommonNumericNullableType[T1, T2]

  trait Div[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped / col2.untyped).typed[Out]
  object Div:
    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Div[T1, T2] with
      type Out = DoubleType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Div[T1, T2] with
      type Out = DoubleOptType

  trait PlusPlus[T1 <: DataType, T2 <: DataType]:
    type Out <: DataType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = concat(col1.untyped, col2.untyped).typed[Out]
  object PlusPlus:
    given stringNonNullable: PlusPlus[StringType, StringType] with
      type Out = StringType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: PlusPlus[T1, T2] with
      type Out = StringOptType

  trait Eq[T1 <: DataType, T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped === col2.untyped).typed[Out]
  object Eq:
    given booleanNonNullable: Eq[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Eq[T1, T2] with
      type Out = BooleanOptType
    
    given stringNonNullable: Eq[StringType, StringType] with
      type Out = BooleanType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: Eq[T1, T2] with
      type Out = BooleanOptType

    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Eq[T1, T2] with
      type Out = BooleanType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Eq[T1, T2] with
      type Out = BooleanOptType

    given structNonNullable[S1 <: Tuple, S2 <: Tuple]: Eq[StructType[S1], StructType[S2]] with
      type Out = BooleanType
    given structNullable[S1 <: Tuple, T1 <: StructOptType[S1], S2 <: Tuple, T2 <: StructOptType[S2]]: Eq[T1, T2] with
      type Out = BooleanOptType

  trait Ne[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped =!= col2.untyped).typed[Out]
  object Ne:
    given booleanNonNullable: Ne[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Ne[T1, T2] with
      type Out = BooleanOptType
    
    given stringNonNullable: Ne[StringType, StringType] with
      type Out = BooleanType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: Ne[T1, T2] with
      type Out = BooleanOptType

    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Ne[T1, T2] with
      type Out = BooleanType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Ne[T1, T2] with
      type Out = BooleanOptType

    given structNonNullable[S1 <: Tuple, S2 <: Tuple]: Ne[StructType[S1], StructType[S2]] with
      type Out = BooleanType
    given structNullable[S1 <: Tuple, T1 <: StructOptType[S1], S2 <: Tuple, T2 <: StructOptType[S2]]: Ne[T1, T2] with
      type Out = BooleanOptType

  trait Lt[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped < col2.untyped).typed[Out]
  object Lt:
    given booleanNonNullable: Lt[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Lt[T1, T2] with
      type Out = BooleanOptType
    
    given stringNonNullable: Lt[StringType, StringType] with
      type Out = BooleanType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: Lt[T1, T2] with
      type Out = BooleanOptType

    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Lt[T1, T2] with
      type Out = BooleanType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Lt[T1, T2] with
      type Out = BooleanOptType

    given structNonNullable[S1 <: Tuple, S2 <: Tuple]: Lt[StructType[S1], StructType[S2]] with
      type Out = BooleanType
    given structNullable[S1 <: Tuple, T1 <: StructOptType[S1], S2 <: Tuple, T2 <: StructOptType[S2]]: Lt[T1, T2] with
      type Out = BooleanOptType

  trait Le[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped <= col2.untyped).typed[Out]
  object Le:
    given booleanNonNullable: Le[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Le[T1, T2] with
      type Out = BooleanOptType
    
    given stringNonNullable: Le[StringType, StringType] with
      type Out = BooleanType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: Le[T1, T2] with
      type Out = BooleanOptType

    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Le[T1, T2] with
      type Out = BooleanType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Le[T1, T2] with
      type Out = BooleanOptType

    given structNonNullable[S1 <: Tuple, S2 <: Tuple]: Le[StructType[S1], StructType[S2]] with
      type Out = BooleanType
    given structNullable[S1 <: Tuple, T1 <: StructOptType[S1], S2 <: Tuple, T2 <: StructOptType[S2]]: Le[T1, T2] with
      type Out = BooleanOptType

  trait Gt[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped > col2.untyped).typed[Out]
  object Gt:
    given booleanNonNullable: Gt[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Gt[T1, T2] with
      type Out = BooleanOptType
    
    given stringNonNullable: Gt[StringType, StringType] with
      type Out = BooleanType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: Gt[T1, T2] with
      type Out = BooleanOptType

    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Gt[T1, T2] with
      type Out = BooleanType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Gt[T1, T2] with
      type Out = BooleanOptType

    given structNonNullable[S1 <: Tuple, S2 <: Tuple]: Gt[StructType[S1], StructType[S2]] with
      type Out = BooleanType
    given structNullable[S1 <: Tuple, T1 <: StructOptType[S1], S2 <: Tuple, T2 <: StructOptType[S2]]: Gt[T1, T2] with
      type Out = BooleanOptType

  trait Ge[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped >= col2.untyped).typed[Out]
  object Ge:
    given booleanNonNullable: Ge[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Ge[T1, T2] with
      type Out = BooleanOptType
    
    given stringNonNullable: Ge[StringType, StringType] with
      type Out = BooleanType
    given stringNullable[T1 <: StringOptType, T2 <: StringOptType]: Ge[T1, T2] with
      type Out = BooleanOptType

    given numericNonNullable[T1 <: NumericType, T2 <: NumericType]: Ge[T1, T2] with
      type Out = BooleanType
    given numericNullable[T1 <: NumericOptType, T2 <: NumericOptType]: Ge[T1, T2] with
      type Out = BooleanOptType

    given structNonNullable[S1 <: Tuple, S2 <: Tuple]: Ge[StructType[S1], StructType[S2]] with
      type Out = BooleanType
    given structNullable[S1 <: Tuple, T1 <: StructOptType[S1], S2 <: Tuple, T2 <: StructOptType[S2]]: Ge[T1, T2] with
      type Out = BooleanOptType

  trait And[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped && col2.untyped).typed[Out]
  object And:
    given booleanNonNullable: And[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: And[T1, T2] with
      type Out = BooleanOptType

  trait Or[-T1 <: DataType, -T2 <: DataType]:
    type Out <: BooleanOptType
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = (col1.untyped || col2.untyped).typed[Out]
  object Or:
    given booleanNonNullable: Or[BooleanType, BooleanType] with
      type Out = BooleanType
    given booleanNullable[T1 <: BooleanOptType, T2 <: BooleanOptType]: Or[T1, T2] with
      type Out = BooleanOptType
