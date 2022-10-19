package org.virtuslab.iskra

import org.apache.spark.sql.{ Column => UntypedColumn}
import types.DataType

abstract class Column(val untyped: UntypedColumn)

object Column {
  implicit class NamingOps[T <: DataType, C[_] <: Column](col: C[T]) {
    def as[N <: Name](name: N)(implicit v: ValueOf[N], k: Kind[C]): k.Out[T] As N =
      As[N](k.wrap[T](col.untyped.as(v.value)))
    def alias[N <: Name](name: N)(implicit v: ValueOf[N], k: Kind[C]): k.Out[T] As N =
      As[N](k.wrap[T](col.untyped.as(v.value)))
  }

  implicit class BinaryOps[C1 <: Column](col1: C1) {
    def +(col2: Column)(implicit p: Plus[C1, col2.type]): p.Result = p(col1, col2)
    // inline def +[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Plus[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def -[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Minus[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def *[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Mult[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def /[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Div[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def ++[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.PlusPlus[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def <[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Lt[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def <=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Le[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def >[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Gt[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def >=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Ge[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def ===[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Eq[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def =!=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Ne[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def &&[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.And[T1, T2]): Column[op.Out] = op(col1, col2)
    // inline def ||[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Or[T1, T2]): Column[op.Out] = op(col1, col2)
  }
}

sealed abstract class TypedCol[+T](untyped: UntypedColumn) extends Column(untyped)

  //inline def name(using v: ValueOf[Name]): Name = v.value

final class DataCol[+T](untyped: UntypedColumn) extends TypedCol[T](untyped)
object DataCol {
  def apply[T](untyped: UntypedColumn) = new DataCol[T](untyped)
}

sealed class AggCol[+T](untyped: UntypedColumn) extends TypedCol[T](untyped)
object AggCol {
  def apply[T](untyped: UntypedColumn) = new AggCol[T](untyped)
}

final class SimpleAggCol[+T](untyped: UntypedColumn) extends AggCol[T](untyped)
object SimpleAggCol {
  def apply[T](untyped: UntypedColumn) = new SimpleAggCol[T](untyped)
}

final class WindowCol[+T](untyped: UntypedColumn) extends TypedCol[T](untyped)
object WindowCol {
  def apply[T](untyped: UntypedColumn) = new WindowCol[T](untyped)
}

final class UniversalCol[+T](untyped: UntypedColumn) extends TypedCol[T](untyped)
object UniversalCol {
  def apply[T](untyped: UntypedColumn) = new UniversalCol[T](untyped)
}

trait Alias[N <: Name]

// trait AliasAs[C <: Column] {
//   def alias[N <: Name]
// }

// @annotation.showAsInfix
// class As[C <: Column, N <: Name](underlying: C) extends AnyVal

trait Kind[C[_]] {
  type Out[T] <: TypedCol[T]
  def wrap[T](untyped: UntypedColumn): Out[T]
}

object Kind {
  implicit object data extends Kind[DataCol] {
    type Out[T] = DataCol[T]
    def wrap[T](untyped: UntypedColumn) = DataCol[T](untyped)
  }

  implicit object simpleAgg extends Kind[SimpleAggCol] {
    type Out[T] = SimpleAggCol[T]
    def wrap[T](untyped: UntypedColumn) = SimpleAggCol[T](untyped)
  }
  implicit object agg extends Kind[AggCol] {
    type Out[T] = AggCol[T]
    def wrap[T](untyped: UntypedColumn) = AggCol[T](untyped)
  }
  implicit object universal extends Kind[UniversalCol] {
    type Out[T] = UniversalCol[T]
    def wrap[T](untyped: UntypedColumn) = UniversalCol[T](untyped)
  }
}

trait CommonKind[C1[_], C2[_]] {
  type Out[_]
  def wrap[T](untyped: UntypedColumn): Out[T]
}

object CommonKind extends CommonKindLowPrio {
  implicit object data extends CommonKind[DataCol, DataCol] {
    type Out[T] = DataCol[T]
    def wrap[T](untyped: UntypedColumn) = DataCol[T](untyped)
  }
  implicit object simpleAgg extends CommonKind[SimpleAggCol, SimpleAggCol] {
    type Out[T] = SimpleAggCol[T]
    def wrap[T](untyped: UntypedColumn) = SimpleAggCol[T](untyped)
  }
  implicit object agg extends CommonKind[AggCol, AggCol] {
    type Out[T] = AggCol[T]
    def wrap[T](untyped: UntypedColumn) = AggCol[T](untyped)
  }
  implicit object universal extends CommonKind[UniversalCol, UniversalCol] {
    type Out[T] = UniversalCol[T]
    def wrap[T](untyped: UntypedColumn) = UniversalCol[T](untyped)
  }
}

trait CommonKindLowPrio {
  implicit def promoteUniversal1[C[_]]: CommonKind[UniversalCol, C] { type Out[T] = C[T] } = new CommonKind[UniversalCol, C] {
    type Out[T] = C[T]
    def wrap[T](untyped: UntypedColumn) = (new Column((untyped)){}).asInstanceOf[C[T]]
  }

  implicit def promoteUniversal2[C[_]]: CommonKind[C, UniversalCol] { type Out[T] = C[T] } = new CommonKind[C, UniversalCol] {
    type Out[T] = C[T]
    def wrap[T](untyped: UntypedColumn) = (new Column((untyped)){}).asInstanceOf[C[T]]
  }
}

trait IsAggregatable[-C[_]]
object IsAggregatable {
  implicit object universal extends IsAggregatable[UniversalCol]
  implicit object data extends IsAggregatable[DataCol]
}




// object Column:
//   extension [T <: DataType](col: Column[T])
//     inline def as[N <: Name](name: N)(using v: ValueOf[N]): LabeledColumn[N, T] =
//       LabeledColumn[N, T](col.untyped.as(v.value))
//     inline def alias[N <: Name](name: N)(using v: ValueOf[N]): LabeledColumn[N, T] =
//       LabeledColumn[N, T](col.untyped.as(v.value))

//   extension [T1 <: DataType](col1: Column[T1])
//     inline def +[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Plus[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def -[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Minus[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def *[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Mult[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def /[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Div[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def ++[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.PlusPlus[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def <[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Lt[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def <=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Le[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def >[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Gt[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def >=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Ge[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def ===[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Eq[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def =!=[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Ne[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def &&[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.And[T1, T2]): Column[op.Out] = op(col1, col2)
//     inline def ||[T2 <: DataType](col2: Column[T2])(using op: ColumnOp.Or[T1, T2]): Column[op.Out] = op(col1, col2)



// @annotation.showAsInfix
// class :=[L <: LabeledColumn.Label, T <: DataType](untyped: UntypedColumn) extends Column[T](untyped)

// @annotation.showAsInfix
// trait /[+Prefix <: Name, +Suffix <: Name]

// type LabeledColumn[L <: LabeledColumn.Label, T <: DataType] = :=[L, T]

// object LabeledColumn:
//   type Label = Name | (Name / Name)
//   def apply[L <: LabeledColumn.Label, T <: DataType](untyped: UntypedColumn) = new :=[L, T](untyped)
