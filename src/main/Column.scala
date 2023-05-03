package org.virtuslab.iskra

import org.apache.spark.sql.{ Column => UntypedColumn}
import types.DataType

class Column[+T <: DataType](val untyped: UntypedColumn):

  inline def name(using v: ValueOf[Name]): Name = v.value

object Column:
  extension [T <: DataType](col: Column[T])
    inline def as[N <: Name](name: N)(using v: ValueOf[N]): LabeledColumn[N, T] =
      LabeledColumn[N, T](col.untyped.as(v.value))
    inline def alias[N <: Name](name: N)(using v: ValueOf[N]): LabeledColumn[N, T] =
      LabeledColumn[N, T](col.untyped.as(v.value))

  extension [T1 <: DataType](col1: Column[T1])
    inline def +[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Plus[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def -[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Minus[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def *[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Mult[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def /[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Div[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def ++[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.PlusPlus[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def <[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Lt[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def <=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Le[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def >[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Gt[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def >=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Ge[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def ===[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Eq[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def =!=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Ne[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def &&[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.And[T1, T2]): Column[op.Out] = op(col1, col2)
    inline def ||[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Or[T1, T2]): Column[op.Out] = op(col1, col2)

trait NamedColumn[T <: DataType]:
  self: Column[T] =>

@annotation.showAsInfix
class :=[L <: LabeledColumn.Label, T <: DataType](untyped: UntypedColumn) extends Column[T](untyped) with NamedColumn[T]

@annotation.showAsInfix
trait /[+Prefix <: Name, +Suffix <: Name]

type LabeledColumn[L <: LabeledColumn.Label, T <: DataType] = :=[L, T]

object LabeledColumn:
  type Label = Name | (Name / Name)
  def apply[L <: LabeledColumn.Label, T <: DataType](untyped: UntypedColumn) = new :=[L, T](untyped)
