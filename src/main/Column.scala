package org.virtuslab.iskra

import scala.language.implicitConversions

import scala.quoted.*

import org.apache.spark.sql.{Column => UntypedColumn}
import types.DataType
import MacroHelpers.TupleSubtype

class Column(val untyped: UntypedColumn):
  inline def name(using v: ValueOf[Name]): Name = v.value

object Column:
  implicit transparent inline def columnToNamedColumn(inline col: Col[?]): NamedColumn[?, ?] =
    ${ columnToNamedColumnImpl('col) }

  private def columnToNamedColumnImpl(col: Expr[Col[?]])(using Quotes): Expr[NamedColumn[?, ?]] =
    import quotes.reflect.*
    col match
      case '{ ($v: StructuralSchemaView).selectDynamic($nm: Name).$asInstanceOf$[Col[tp]] } =>
        nm.asTerm.tpe.asType match
          case '[Name.Subtype[n]] =>
            '{ NamedColumn[n, tp](${ col }.untyped.as(${ nm })) }
      case '{ $c: Col[tp] } =>
        col.asTerm match
          case Inlined(_, _, Ident(name)) =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] =>
                val alias = Literal(StringConstant(name)).asExprOf[Name]
                '{ NamedColumn[n, tp](${ col }.untyped.as(${ alias })) }

  extension [T <: DataType](col: Col[T])
    inline def as[N <: Name](name: N): NamedColumn[N, T] =
      NamedColumn[N, T](col.untyped.as(name))
    inline def alias[N <: Name](name: N): NamedColumn[N, T] =
      NamedColumn[N, T](col.untyped.as(name))

  extension [T1 <: DataType](col1: Col[T1])
    inline def +[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Plus[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def -[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Minus[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def *[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Mult[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def /[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Div[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def ++[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.PlusPlus[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def <[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Lt[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def <=[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Le[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def >[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Gt[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def >=[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Ge[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def ===[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Eq[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def =!=[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Ne[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def &&[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.And[T1, T2]): Col[op.Out] = op(col1, col2)
    inline def ||[T2 <: DataType](col2: Col[T2])(using op: ColumnOp.Or[T1, T2]): Col[op.Out] = op(col1, col2)


class Col[+T <: DataType](untyped: UntypedColumn) extends Column(untyped)


object Columns:
  transparent inline def apply[C <: NamedColumns](columns: C): ColumnsWithSchema[?] = ${ applyImpl('columns) }

  private def applyImpl[C : Type](columns: Expr[C])(using Quotes): Expr[ColumnsWithSchema[?]] =
    import quotes.reflect.*

    Expr.summon[CollectColumns[C]] match
      case Some(collectColumns) =>
        collectColumns match
          case '{ $cc: CollectColumns[?] { type CollectedColumns = collectedColumns } } =>
            Type.of[collectedColumns] match
              case '[TupleSubtype[collectedCols]] =>
                '{
                  val cols = ${ cc }.underlyingColumns(${ columns })
                  ColumnsWithSchema[collectedCols](cols)
                }
      case None =>
        throw CollectColumns.CannotCollectColumns(Type.show[C])


trait NamedColumnOrColumnsLike

type NamedColumns = Repeated[NamedColumnOrColumnsLike]

class NamedColumn[N <: Name, T <: DataType](val untyped: UntypedColumn)
  extends NamedColumnOrColumnsLike

class ColumnsWithSchema[Schema <: Tuple](val underlyingColumns: Seq[UntypedColumn]) extends NamedColumnOrColumnsLike


@annotation.showAsInfix
trait :=[L <: ColumnLabel, T <: DataType]

@annotation.showAsInfix
trait /[+Prefix <: Name, +Suffix <: Name]

type ColumnLabel = Name | (Name / Name)
