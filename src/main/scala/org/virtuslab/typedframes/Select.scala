package org.virtuslab.typedframes

import scala.quoted.*
import types.{ DataType, StructType }

class Select[V <: SchemaView](view: V, underlying: UntypedDataFrame):
  import Select.SelectColumns
  def apply[T](selectedColumns: V ?=> T)(using mc: SelectColumns[T]): DataFrame[mc.OutputSchema] =
    val typedCols = selectedColumns(using view)
    val columns  = mc.columns(typedCols)
    DataFrame[mc.OutputSchema](underlying.select(columns*))

object Select:
  given selectOps: {} with
    extension [DF <: DataFrame[?]](tdf: DF)
      transparent inline def select: Select[?] = ${ Select.selectImpl[DF]('{tdf}) }

  def selectImpl[DF <: DataFrame[?] : Type](tdf: Expr[DF])(using Quotes): Expr[Select[?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[DF]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{ Select[v](${ viewExpr }.asInstanceOf[v], ${ tdf }.untyped) }

  trait SelectColumns[T]:
    type OutputSchema <: FrameSchema
    def columns(t: T): List[UntypedColumn]

  object SelectColumns:
    transparent inline given selectSingleColumn[N <: Name, A <: DataType]: SelectColumns[LabeledColumn[N, A]] =
      new SelectColumns[LabeledColumn[N, A]]:
        type OutputSchema = Tuple1[LabeledColumn[N, A]]
        def columns(t: LabeledColumn[N, A]): List[UntypedColumn] = List(t.untyped)

    transparent inline given selectMultipleColumns[T <: Tuple]: SelectColumns[T] =
      new SelectColumns[T]:
        type OutputSchema = T
        def columns(t: T): List[UntypedColumn] = t.toList.map(col => col.asInstanceOf[TypedColumn[DataType]].untyped)
