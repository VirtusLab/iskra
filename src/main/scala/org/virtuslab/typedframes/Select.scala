package org.virtuslab.typedframes

import scala.quoted.*
import org.apache.spark.sql.{ Column => UntypedColumn, DataFrame => UntypedDataFrame }
import types.{ DataType, StructType }
import Internals.Name

trait SelectCtx extends SparkOpCtx:
  type CtxOut <: SelectionView

extension [DF <: TypedDataFrame[?]](tdf: DF)
  transparent inline def select: SelectOps[?] = ${ SelectOps.selectImpl[DF]('{tdf}) }

class SelectOps[View <: SelectionView](view: View, underlying: UntypedDataFrame):
  def apply[T](f: SelectCtx { type CtxOut = View } ?=> T)(using mc: MergeColumns[T]): TypedDataFrame[mc.MergedSchema] =
    val ctx = new SelectCtx {
      type CtxOut = View
      def ctxOut: View = view
    }
    val typedCols = f(using ctx)
    val columns  = mc.columns(typedCols)
    (underlying.select(columns*)).withSchema[mc.MergedSchema]

object SelectOps:
  def selectImpl[DF <: TypedDataFrame[?] : Type](tdf: Expr[DF])(using Quotes): Expr[SelectOps[?]] =
    import quotes.reflect.*
    val viewExpr = SelectionView.selectionViewExpr[DF]//(tdf)
    viewExpr.asTerm.tpe.asType match
      case '[v] =>
        '{ SelectOps[v & SelectionView](${ viewExpr }.asInstanceOf[v & SelectionView], ${ tdf }.untyped) }

trait MergeColumns[T]:
  type MergedSchema <: StructType
  def columns(t: T): List[UntypedColumn]

object MergeColumns:
  type ColumnsNames[T <: Tuple] <: Tuple = T match
    case NamedColumn[name, tpe] *: tail => name *: ColumnsNames[tail]
    case EmptyTuple => EmptyTuple

  type ColumnsTypes[T <: Tuple] <: Tuple = T match
    case TypedColumn[tpe] *: tail => tpe *: ColumnsTypes[tail]
    case EmptyTuple => EmptyTuple

  transparent inline given mergeTupleColumns[T <: Tuple]: MergeColumns[T] = ${ mergeTupleColumnsImpl[T] }

  def mergeTupleColumnsImpl[T <: Tuple : Type](using Quotes): Expr[MergeColumns[T]] = 
    '{
      type s = StructType.FromLabelsAndTypes[ColumnsNames[T], ColumnsTypes[T]]
      new MergeColumns[T] {
        type MergedSchema = s
        def columns(t: T): List[UntypedColumn] = t.toList.map(col => col.asInstanceOf[TypedColumn[DataType]].untyped)
      }.asInstanceOf[MergeColumns[T] {type MergedSchema = s}]
    }

  // TODO: assure N is not just Internals.Name or Nothing
  transparent inline given mergeSingleColumn[N <: Name, A <: DataType]: MergeColumns[NamedColumn[N, A]] = ${ mergeSingleColumnImpl[N, A] }

  def mergeSingleColumnImpl[N <: Name : Type, A <: DataType : Type](using Quotes): Expr[MergeColumns[NamedColumn[N, A]]] =
    '{
      type s = StructType.WithSingleColumn[N, A]
      new MergeColumns[NamedColumn[N, A]] {
        type MergedSchema = s
        def columns(t: NamedColumn[N, A]): List[UntypedColumn] = List(t.untyped)
      }.asInstanceOf[MergeColumns[NamedColumn[N, A]] {type MergedSchema = s}]
    }