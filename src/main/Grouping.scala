package org.virtuslab.typedframes

import scala.quoted.*
import org.virtuslab.typedframes.types.DataType
import MacroHelpers.TupleSubtype

class GroupBy[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object GroupBy:
  given dataFrameGroupByOps: {} with
    extension [S] (df: DataFrame[S])
      transparent inline def groupBy: GroupBy[?] = ${ GroupBy.groupByImpl[S]('{df}) }

  given groupByOps: {} with
    extension [View <: SchemaView](groupBy: GroupBy[View])
      transparent inline def apply[GroupingColumns](groupingColumns: View ?=> GroupingColumns) = ${ applyImpl[View, GroupingColumns]('groupBy, 'groupingColumns) }

  def groupByImpl[S : Type](df: Expr[DataFrame[S]])(using Quotes): Expr[GroupBy[?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[DataFrame[S]]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{ GroupBy[v](${ viewExpr }.asInstanceOf[v], ${ df }.untyped) }

  def applyImpl[View <: SchemaView : Type, GroupingColumns : Type](groupBy: Expr[GroupBy[View]], groupingColumns: Expr[View ?=> GroupingColumns])(using Quotes): Expr[GroupedDataFrame[View]] =
    import quotes.reflect.*
    Type.of[GroupingColumns] match
      case '[name := colType] =>
        val groupedViewExpr = SchemaView.schemaViewExpr[DataFrame[name := colType]]
        groupedViewExpr.asTerm.tpe.asType match
          case '[SchemaView.Subtype[gv]] =>
            '{
              new GroupedDataFrame[View]:
                type GroupingKeys = (name := colType) *: EmptyTuple
                type GroupedView = gv
                def underlying = ${ groupBy }.underlying.groupBy(${ groupingColumns }(using ${ groupBy }.view).asInstanceOf[name := colType].untyped)
                def fullView = ${ groupBy }.view
                def groupedView = ${ groupedViewExpr }.asInstanceOf[GroupedView]
            }

      case '[TupleSubtype[s]] if FrameSchema.isValidType(Type.of[s]) =>
        val groupedViewExpr = SchemaView.schemaViewExpr[DataFrame[s]]
        groupedViewExpr.asTerm.tpe.asType match
          case '[SchemaView.Subtype[gv]] =>
            '{
              val groupingCols = ${ groupingColumns }(using ${ groupBy }.view).asInstanceOf[s].toList.map(_.asInstanceOf[Column[DataType]].untyped)
              new GroupedDataFrame[View]:
                type GroupingKeys = s
                type GroupedView = gv
                def underlying = ${ groupBy }.underlying.groupBy(groupingCols*)
                def fullView = ${ groupBy }.view
                def groupedView = ${ groupedViewExpr }.asInstanceOf[GroupedView]
            }

      case '[t] =>
        val errorMsg = s"""The parameter of `groupBy` should be a named column (e.g. of type: "foo" := StringType) or a tuple of named columns but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(groupBy))

// TODO: Rename to RelationalGroupedDataset and handle other aggregations: cube, rollup (and pivot?)
trait GroupedDataFrame[FullView <: SchemaView]:
  import GroupedDataFrame.*

  type GroupingKeys <: Tuple
  type GroupedView <: SchemaView
  def underlying: UntypedRelationalGroupedDataset
  def fullView: FullView
  def groupedView: GroupedView

object GroupedDataFrame:
  given groupedDataFrameOps: {} with
    extension [FullView <: SchemaView, GroupKeys <: Tuple, GroupView <: SchemaView](gdf: GroupedDataFrame[FullView]{ type GroupedView = GroupView; type GroupingKeys = GroupKeys })
      transparent inline def agg[AggregatedColumns](columns: Agg { type View = FullView } ?=> GroupView ?=> AggregatedColumns): DataFrame[?] =
        ${ aggImpl[FullView, GroupKeys, GroupView, AggregatedColumns]('gdf, 'columns) }

  def aggImpl[FullView <: SchemaView : Type, GroupingKeys <: Tuple : Type, GroupView <: SchemaView : Type, AggregatedColumns : Type](
    gdf: Expr[GroupedDataFrame[FullView] { type GroupedView = GroupView }],
    columns: Expr[Agg { type View = FullView } ?=> GroupView ?=> AggregatedColumns]
  )(using Quotes): Expr[DataFrame[?]] =
    import quotes.reflect.*
    val aggWrapper = '{
      new Agg:
        type View = FullView
        val view = ${ gdf }.fullView
    }
    Type.of[AggregatedColumns] match
      case '[name := colType] =>
        '{
          val col = ${ columns }(using ${ aggWrapper })(using ${ gdf }.groupedView).asInstanceOf[name := colType].untyped
          DataFrame[FrameSchema.Merge[GroupingKeys, name := colType]](
            ${ gdf }.underlying.agg(col)
          )
        }
      case '[TupleSubtype[s]] if FrameSchema.isValidType(Type.of[s]) =>
        '{
          val cols = ${ columns }(using ${ aggWrapper })(using ${ gdf }.groupedView)
            .asInstanceOf[s].toList.map(_.asInstanceOf[Column[DataType]].untyped)
          DataFrame[FrameSchema.Merge[GroupingKeys, s]](
            ${ gdf }.underlying.agg(cols.head, cols.tail*)
          )
        }
      case '[t] =>
        val errorMsg = s"""The parameter of `agg` should be a named column (e.g. of type: "foo" := StringType) or a tuple of named columns but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(gdf))

trait Agg:
  type View <: SchemaView
  def view: View
