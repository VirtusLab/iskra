package org.virtuslab.iskra

import scala.quoted.*
import org.virtuslab.iskra.types.DataType
import MacroHelpers.TupleSubtype

class GroupBy[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object GroupBy:
  given dataFrameGroupByOps: {} with
    extension [S] (df: StructDataFrame[S])
      transparent inline def groupBy: GroupBy[?] = ${ GroupBy.groupByImpl[S]('{df}) }

  given groupByOps: {} with
    extension [View <: SchemaView](groupBy: GroupBy[View])
      transparent inline def apply[C <: Repeated[NamedColumns[?]]](groupingColumns: View ?=> C) = ${ applyImpl[View, C]('groupBy, 'groupingColumns) }

  private def groupByImpl[S : Type](df: Expr[StructDataFrame[S]])(using Quotes): Expr[GroupBy[?]] =
    import quotes.reflect.asTerm
    val viewExpr = StructSchemaView.schemaViewExpr[StructDataFrame[S]]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{ GroupBy[v](${ viewExpr }.asInstanceOf[v], ${ df }.untyped) }

  private def applyImpl[View <: SchemaView : Type, C : Type](groupBy: Expr[GroupBy[View]], groupingColumns: Expr[View ?=> C])(using Quotes): Expr[GroupedDataFrame[View]] =
    import quotes.reflect.*

    Expr.summon[CollectColumns[C]] match
      case Some(collectColumns) =>
        collectColumns match
          case '{ $cc: CollectColumns[?] { type CollectedColumns = collectedColumns } } =>
            Type.of[collectedColumns] match
              case '[TupleSubtype[collectedCols]] =>
                val groupedViewExpr = StructSchemaView.schemaViewExpr[StructDataFrame[collectedCols]]
                groupedViewExpr.asTerm.tpe.asType match
                  case '[SchemaView.Subtype[groupedView]] =>
                    '{
                      val groupingCols = ${ cc }.underlyingColumns(${ groupingColumns }(using ${ groupBy }.view))
                      new GroupedDataFrame[View]:
                        type GroupingKeys = collectedCols
                        type GroupedView = groupedView
                        def underlying = ${ groupBy }.underlying.groupBy(groupingCols*)
                        def fullView = ${ groupBy }.view
                        def groupedView = ${ groupedViewExpr }.asInstanceOf[GroupedView]
                    }
      case None =>
        throw CollectColumns.CannotCollectColumns(Type.show[C])

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
      transparent inline def agg[C <: Repeated[NamedColumns[?]]](columns: (Agg { type View = FullView }, GroupView) ?=> C): StructDataFrame[?] =
        ${ aggImpl[FullView, GroupKeys, GroupView, C]('gdf, 'columns) }

  private def aggImpl[FullView <: SchemaView : Type, GroupingKeys <: Tuple : Type, GroupView <: SchemaView : Type, C : Type](
    gdf: Expr[GroupedDataFrame[FullView] { type GroupedView = GroupView }],
    columns: Expr[(Agg { type View = FullView }, GroupView) ?=> C]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*

    val aggWrapper = '{
      new Agg:
        type View = FullView
        val view = ${ gdf }.fullView
    }

    Expr.summon[CollectColumns[C]] match
      case Some(collectColumns) =>
        collectColumns match
          case '{ $cc: CollectColumns[?] { type CollectedColumns = collectedColumns } } =>
            '{
              // TODO assert cols is not empty
              val cols = ${ cc }.underlyingColumns(${ columns }(using ${ aggWrapper }, ${ gdf }.groupedView))
              StructDataFrame[FrameSchema.Merge[GroupingKeys, collectedColumns]](
                ${ gdf }.underlying.agg(cols.head, cols.tail*)
              )
            }
      case None =>
        throw CollectColumns.CannotCollectColumns(Type.show[C])

trait Agg:
  type View <: SchemaView
  def view: View
