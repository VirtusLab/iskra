package org.virtuslab.iskra

import scala.quoted.*
import org.apache.spark.sql
import org.apache.spark.sql.functions.concat
import org.virtuslab.iskra.Col
import org.virtuslab.iskra.UntypedOps.typed
import org.virtuslab.iskra.types.*
import DataType.*

trait ColumnOp:
  type Out <: DataType

object ColumnOp:
  trait ResultType[T <: DataType] extends ColumnOp:
    override type Out = T

  abstract class BinaryColumnOp[T1 <: DataType, T2 <: DataType](untypedOp: (UntypedColumn, UntypedColumn) => UntypedColumn) extends ColumnOp:
    def apply(col1: Col[T1], col2: Col[T2]): Col[Out] = untypedOp(col1.untyped, col2.untyped).typed[Out]

  class Plus[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ + _)
  object Plus:
    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Plus[T1, T2] { type Out = CommonNumericType[T1, T2] }) =
      new Plus[T1, T2] with ResultType[CommonNumericType[T1, T2]]

  class Minus[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ - _)
  object Minus:
    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Minus[T1, T2] { type Out = CommonNumericType[T1, T2] }) =
      new Minus[T1, T2] with ResultType[CommonNumericType[T1, T2]]
      
  class Mult[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ * _)
  object Mult:
    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Mult[T1, T2] { type Out = CommonNumericType[T1, T2] }) =
      new Mult[T1, T2] with ResultType[CommonNumericType[T1, T2]]

  class Div[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ / _)
  object Div:
    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Div[T1, T2] { type Out = DoubleOfCommonNullability[T1, T2] }) =
      new Div[T1, T2] with ResultType[DoubleOfCommonNullability[T1, T2]]

  class PlusPlus[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](concat(_, _))
  object PlusPlus:
    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (PlusPlus[T1, T2] { type Out = StringOfCommonNullability[T1, T2] }) =
      new PlusPlus[T1, T2] with ResultType[StringOfCommonNullability[T1, T2]]

  class Eq[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ === _)
  object Eq:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Eq[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Eq[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (Eq[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Eq[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Eq[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Eq[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given structs[S1 <: Tuple, S2 <: Tuple, T1 <: StructOptLike[S1], T2 <: StructOptLike[S2]]: (Eq[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Eq[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

  class Ne[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ =!= _)
  object Ne:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Ne[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Ne[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (Ne[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Ne[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Ne[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Ne[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given structs[S1 <: Tuple, S2 <: Tuple, T1 <: StructOptLike[S1], T2 <: StructOptLike[S2]]: (Ne[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Ne[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

  class Lt[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ < _)
  object Lt:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Lt[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Lt[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (Lt[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Lt[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Lt[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Lt[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given structs[S1 <: Tuple, S2 <: Tuple, T1 <: StructOptLike[S1], T2 <: StructOptLike[S2]]: (Lt[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Lt[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

  class Le[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ <= _)
  object Le:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Le[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Le[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (Le[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Le[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Le[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Le[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given structs[S1 <: Tuple, S2 <: Tuple, T1 <: StructOptLike[S1], T2 <: StructOptLike[S2]]: (Le[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Le[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

  class Gt[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ > _)
  object Gt:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Gt[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Gt[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (Gt[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Gt[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Gt[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Gt[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given structs[S1 <: Tuple, S2 <: Tuple, T1 <: StructOptLike[S1], T2 <: StructOptLike[S2]]: (Gt[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Gt[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

  class Ge[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ >= _)
  object Ge:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Ge[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Ge[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

    given strings[T1 <: StringOptLike, T2 <: StringOptLike]: (Ge[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Ge[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given numerics[T1 <: DoubleOptLike, T2 <: DoubleOptLike]: (Ge[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Ge[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

    given structs[S1 <: Tuple, S2 <: Tuple, T1 <: StructOptLike[S1], T2 <: StructOptLike[S2]]: (Ge[T1, T2] { type Out = BooleanOfCommonNullability[T1, T2] }) =
      new Ge[T1, T2] with ResultType[BooleanOfCommonNullability[T1, T2]]

  class And[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ && _)
  object And:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (And[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new And[T1, T2] with ResultType[CommonBooleanType[T1, T2]]

  class Or[T1 <: DataType, T2 <: DataType] extends BinaryColumnOp[T1, T2](_ || _)
  object Or:
    given booleans[T1 <: BooleanOptLike, T2 <: BooleanOptLike]: (Or[T1, T2] { type Out = CommonBooleanType[T1, T2] }) =
      new Or[T1, T2] with ResultType[CommonBooleanType[T1, T2]]
