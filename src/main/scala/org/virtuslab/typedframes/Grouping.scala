package org.virtuslab.typedframes

import scala.quoted.*
import org.virtuslab.typedframes.types.DataType

class GroupBy[V <: SchemaView](view: V, underlying: UntypedDataFrame):
  import GroupBy.GroupByColumns
  def apply[T](groupingColumns: V ?=> T)(using gbc: GroupByColumns[T]): GroupedDataFrame[V, gbc.GroupingKeys, gbc.GroupedView] =
    val typedCols = groupingColumns(using view)
    val columns  = gbc.columns(typedCols)
    GroupedDataFrame[V, gbc.GroupingKeys, gbc.GroupedView](underlying.groupBy(columns*), view, gbc.groupedView)

object GroupBy:
  given groupingOps: {} with
    extension [S <: FrameSchema] (df: DataFrame[S])
      transparent inline def groupBy: GroupBy[?] = ${ GroupBy.groupByImpl[S]('{df}) }

  def groupByImpl[S <: FrameSchema : Type](df: Expr[DataFrame[S]])(using Quotes): Expr[GroupBy[?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[DataFrame[S]]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{ GroupBy[v](${ viewExpr }.asInstanceOf[v], ${ df }.untyped) }


  trait GroupByColumns[T]:
    type GroupingKeys <: FrameSchema
    type GroupedView <: SchemaView
    def groupedView: GroupedView
    def columns(t: T): List[UntypedColumn]

  object GroupByColumns:
    transparent inline given groupBySingleColumn[N <: Name, A <: DataType]: GroupByColumns[LabeledColumn[N, A]] = ${ groupBySingleColumnImpl[N, A] }

    def groupBySingleColumnImpl[N <: Name : Type, A <: DataType : Type](using Quotes): Expr[GroupByColumns[LabeledColumn[N, A]]] =
      import quotes.reflect.asTerm
      val viewExpr = SchemaView.schemaViewExpr[DataFrame[Tuple1[LabeledColumn[N, A]]]]
      viewExpr.asTerm.tpe.asType match
        case '[SchemaView.Subtype[v]] =>
          '{
            new GroupByColumns[LabeledColumn[N, A]]:
              type GroupingKeys = Tuple1[LabeledColumn[N, A]]
              type GroupedView = v
              val groupedView = ${ viewExpr }.asInstanceOf[v]
              def columns(t: LabeledColumn[N, A]): List[UntypedColumn] = List(t.untyped)
          }

    transparent inline given groupByMultipleColumns[T <: Tuple]: GroupByColumns[T] = ${ groupByMultipleColumnsImpl[T] }

    def groupByMultipleColumnsImpl[T <: FrameSchema : Type](using Quotes): Expr[GroupByColumns[T]] =
      import quotes.reflect.asTerm
      val viewExpr = SchemaView.schemaViewExpr[DataFrame[T]]
      viewExpr.asTerm.tpe.asType match
        case '[SchemaView.Subtype[v]] =>
          '{
            new GroupByColumns[T]:
              type GroupingKeys = T
              type GroupedView = v
              val groupedView = ${ viewExpr }.asInstanceOf[v]
              def columns(t: T): List[UntypedColumn] = t.toList.map(col => col.asInstanceOf[TypedColumn[DataType]].untyped)
          }

// TODO: Rename to RelationalGroupedDataset and handle other aggregations: cube, rollup (and pivot?)
class GroupedDataFrame[FullView <: SchemaView, GroupingKeys <: FrameSchema, GroupedView <: SchemaView](
  underlying: UntypedRelationalGroupedDataset,
  fullView: FullView,
  groupedView: GroupedView,
):
  import GroupedDataFrame.*

  // TODO: prepend grouping keys (rename to aggregation keys) to the resulting schema
  def agg[T](f: Agg { type View = FullView } ?=> GroupedView ?=> T)(using a: Aggregate[T]): DataFrame[Tuple.Concat[GroupingKeys, a.OutputSchema]] =
    val aggWrapper = new Agg:
      type View = FullView
      val view = fullView
    val typedCols = f(using aggWrapper)(using groupedView)
    val columns = a.columns(typedCols)
    DataFrame[Tuple.Concat[GroupingKeys, a.OutputSchema]](underlying.agg(columns.head, columns.tail*))


object GroupedDataFrame:
  trait Aggregate[T]:
    type OutputSchema <: FrameSchema
    def columns(t: T): List[UntypedColumn]

  transparent inline given aggregateSingleColumn[N <: Name, A <: DataType]: Aggregate[LabeledColumn[N, A]] =
    new Aggregate[LabeledColumn[N, A]]:
      type OutputSchema = Tuple1[LabeledColumn[N, A]]
      def columns(t: LabeledColumn[N, A]): List[UntypedColumn] = List(t.untyped)

  transparent inline given aggregateMultipleColumns[T <: Tuple]: Aggregate[T] =
    new Aggregate[T]:
      type OutputSchema = T
      def columns(t: T): List[UntypedColumn] = t.toList.map(col => col.asInstanceOf[TypedColumn[DataType]].untyped)

// TODO: Make this nested or keep as top level?
trait Agg:
  type View <: SchemaView
  def view: View