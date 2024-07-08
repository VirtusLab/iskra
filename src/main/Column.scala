package org.virtuslab.iskra

import scala.language.implicitConversions

import scala.quoted.*

import org.apache.spark.sql.{Column => UntypedColumn}
import types.DataType

sealed trait NamedColumns[Schema](val underlyingColumns: Seq[UntypedColumn])

object Columns:
  transparent inline def apply(inline columns: NamedColumns[?]*): NamedColumns[?] = ${ applyImpl('columns) }

  private def applyImpl(columns: Expr[Seq[NamedColumns[?]]])(using Quotes): Expr[NamedColumns[?]] =
    import quotes.reflect.*

    val columnValuesWithTypes = columns match
      case Varargs(colExprs) =>
        colExprs.map { arg =>
          arg match
            case '{ $value: NamedColumns[schema] } => ('{ ${ value }.underlyingColumns }, Type.of[schema])
        }

    val columnsValues = columnValuesWithTypes.map(_._1)
    val columnsTypes = columnValuesWithTypes.map(_._2)

    val schemaTpe = FrameSchema.schemaTypeFromColumnsTypes(columnsTypes)

    schemaTpe match
      case '[s] =>
        '{
          val cols = ${ Expr.ofSeq(columnsValues) }.flatten
          new NamedColumns[s](cols) {}
        }

class Column(val untyped: UntypedColumn):
  inline def name(using v: ValueOf[Name]): Name = v.value

object Column:
  implicit transparent inline def columnToLabeledColumn(inline col: Col[?]): LabeledColumn[?, ?] =
    ${ columnToLabeledColumnImpl('col) }

  private def columnToLabeledColumnImpl(col: Expr[Col[?]])(using Quotes): Expr[LabeledColumn[?, ?]] =
    import quotes.reflect.*
    col match
      case '{ ($v: StructuralSchemaView).selectDynamic($nm: Name).$asInstanceOf$[Col[tp]] } =>
        nm.asTerm.tpe.asType match
          case '[Name.Subtype[n]] =>
            '{ LabeledColumn[n, tp](${ col }.untyped.as(${ nm })) }
      case '{ $c: Col[tp] } =>
        col.asTerm match
          case Inlined(_, _, Ident(name)) =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] =>
                val alias = Literal(StringConstant(name)).asExprOf[Name]
                '{ LabeledColumn[n, tp](${ col }.untyped.as(${ alias })) }

  extension [T <: DataType](col: Col[T])
    inline def as[N <: Name](name: N): LabeledColumn[N, T] =
      LabeledColumn[N, T](col.untyped.as(name))
    inline def alias[N <: Name](name: N): LabeledColumn[N, T] =
      LabeledColumn[N, T](col.untyped.as(name))

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

@annotation.showAsInfix
trait :=[L <: LabeledColumn.Label, T <: DataType]

@annotation.showAsInfix
trait /[+Prefix <: Name, +Suffix <: Name]

class LabeledColumn[L <: Name, T <: DataType](untyped: UntypedColumn)
  extends NamedColumns[(L := T) *: EmptyTuple](Seq(untyped))

object LabeledColumn:
  type Label = Name | (Name / Name)
