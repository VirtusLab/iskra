package org.virtuslab.typedframes

import org.apache.spark.sql.{ Column => UntypedColumn}
import types.DataType

class TypedColumn[T <: DataType](val untyped: UntypedColumn) /* extends AnyVal */:
  inline def as[N <: Name](name: N)(using v: ValueOf[N]): LabeledColumn[N, T] =
    LabeledColumn[N, T](untyped.as(v.value))

  inline def name(using v: ValueOf[Name]): Name = v.value

  inline def +[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Plus[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def -[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Minus[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def ++[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.PlusPlus[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def <[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Lt[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def <=[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Le[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def >[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Gt[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def >=[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Ge[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def ===[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Eq[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def =!=[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Ne[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def &&[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.And[T, T2]): TypedColumn[op.Out] = op(this, that)
  inline def ||[T2 <: DataType](that: TypedColumn[T2])(using op: ColumnOp.Or[T, T2]): TypedColumn[op.Out] = op(this, that)

object TypedColumn:
  given untypedColumnOps: {} with
    extension (untyped: UntypedColumn)
      def typed[A <: DataType] = TypedColumn[A](untyped)

class ~>[L <: LabeledColumn.Label, T <: DataType](untyped: UntypedColumn) extends TypedColumn[T](untyped)

type LabeledColumn[L <: LabeledColumn.Label, T <: DataType] = ~>[L, T]

object LabeledColumn:
  type Label = Name | (Name, Name)
  def apply[L <: LabeledColumn.Label, T <: DataType](untyped: UntypedColumn) = new ~>[L, T](untyped)
