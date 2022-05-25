package org.virtuslab.typedframes

import scala.quoted.*
import org.apache.spark.sql.{ Column => UntypedColumn}
import Internals.Name

trait SelectCtx extends SparkOpCtx:
  type CtxOut <: SelectionView

extension [S <: FrameSchema](inline tdf: TypedDataFrame[S])(using sv: SelectionViewFor[S])
  inline def select[T](f: SelectCtx { type CtxOut = sv.View } ?=> T)(using mc: MergeColumns[T]): TypedDataFrame[mc.MergedSchema] =
    val ctx: SelectCtx { type CtxOut = sv.View } = new SelectCtx {
      type CtxOut = sv.View
      def ctxOut: sv.View = SelectionView().asInstanceOf[sv.View]
    }
    val typedCols = f(using ctx)
    val columns  = mc.columns(typedCols)
    (tdf.untyped.select(columns*)).withSchema[mc.MergedSchema]

trait MergeColumns[T]:
  type MergedSchema <: FrameSchema
  def columns(t: T): List[UntypedColumn]

type ColumnsNames[T <: Tuple] <: Tuple = T match
  case TypedColumn[name, tpe] *: tail => name *: ColumnsNames[tail]
  case EmptyTuple => EmptyTuple

type ColumnsTypes[T <: Tuple] <: Tuple = T match
  case TypedColumn[name, tpe] *: tail => tpe *: ColumnsTypes[tail]
  case EmptyTuple => EmptyTuple

transparent inline given mergeTupleColumns[T <: Tuple]: MergeColumns[T] = ${ mergeTupleColumnsImpl[T] }


// TODO:

def mergeTupleColumnsImpl[T <: Tuple : Type](using Quotes): Expr[MergeColumns[T]] = 
  '{
    type s = FrameSchemaFromLabelsAndTypes[ColumnsNames[T], ColumnsTypes[T]]
    new MergeColumns[T] {
      type MergedSchema = s
      def columns(t: T): List[UntypedColumn] = t.toList.map(col => col.asInstanceOf[TypedColumn[Name, Any]].underlying) //List(t.underlying)
    }.asInstanceOf[MergeColumns[T] {type MergedSchema = s}]
  }

// // TODO: can/should we somehow reuse the implementation for tuples?
transparent inline given [N <: Name, A]: MergeColumns[TypedColumn[N, A]] = ${ mergeSingleColumnImpl[N, A] }

def mergeSingleColumnImpl[N <: Name : Type, A : Type](using Quotes): Expr[MergeColumns[TypedColumn[N, A]]] =
  '{
    type s = FrameSchema.WithSingleColumn[N, A]
    new MergeColumns[TypedColumn[N, A]] {
      type MergedSchema = s
      def columns(t: TypedColumn[N, A]): List[UntypedColumn] = List(t.underlying)
    }.asInstanceOf[MergeColumns[TypedColumn[N, A]] {type MergedSchema = s}]
  }