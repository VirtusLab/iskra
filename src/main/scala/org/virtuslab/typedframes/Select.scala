package org.virtuslab.typedframes

import scala.quoted.*
import org.apache.spark.sql.{ Column => UntypedColumn}
import Internals.Name

trait SelectCtx extends SparkOpCtx:
  type CtxOut <: SelectionView

extension [S <: FrameSchema](inline tdf: TypedDataFrame[S])(using svp: SelectionView.Provider[S])
  inline def select[T](f: SelectCtx { type CtxOut = svp.View } ?=> T)(using mc: MergeColumns[T]): TypedDataFrame[mc.MergedSchema] =
    val ctx: SelectCtx { type CtxOut = svp.View } = new SelectCtx {
      type CtxOut = svp.View
      def ctxOut: svp.View = SelectionView().asInstanceOf[svp.View]
    }
    val typedCols = f(using ctx)
    val columns  = mc.columns(typedCols)
    (tdf.untyped.select(columns*)).withSchema[mc.MergedSchema]

trait MergeColumns[T]:
  type MergedSchema <: FrameSchema
  def columns(t: T): List[UntypedColumn]

object MergeColumns:
  type ColumnsNames[T <: Tuple] <: Tuple = T match
    case TypedColumn[name, tpe] *: tail => name *: ColumnsNames[tail]
    case EmptyTuple => EmptyTuple

  type ColumnsTypes[T <: Tuple] <: Tuple = T match
    case TypedColumn[name, tpe] *: tail => tpe *: ColumnsTypes[tail]
    case EmptyTuple => EmptyTuple

  transparent inline given mergeTupleColumns[T <: Tuple]: MergeColumns[T] = ${ mergeTupleColumnsImpl[T] }

  def mergeTupleColumnsImpl[T <: Tuple : Type](using Quotes): Expr[MergeColumns[T]] = 
    '{
      type s = FrameSchema.FromLabelsAndTypes[ColumnsNames[T], ColumnsTypes[T]]
      new MergeColumns[T] {
        type MergedSchema = s
        def columns(t: T): List[UntypedColumn] = t.toList.map(col => col.asInstanceOf[TypedColumn[Name, Any]].underlying) //List(t.underlying)
      }.asInstanceOf[MergeColumns[T] {type MergedSchema = s}]
    }

  transparent inline given mergeSingleColumn[N <: Name, A]: MergeColumns[TypedColumn[N, A]] = ${ mergeSingleColumnImpl[N, A] }

  def mergeSingleColumnImpl[N <: Name : Type, A : Type](using Quotes): Expr[MergeColumns[TypedColumn[N, A]]] =
    '{
      type s = FrameSchema.WithSingleColumn[N, A]
      new MergeColumns[TypedColumn[N, A]] {
        type MergedSchema = s
        def columns(t: TypedColumn[N, A]): List[UntypedColumn] = List(t.underlying)
      }.asInstanceOf[MergeColumns[TypedColumn[N, A]] {type MergedSchema = s}]
    }