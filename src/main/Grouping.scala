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
      transparent inline def apply(inline groupingColumns: View ?=> NamedColumns[?]*) = ${ applyImpl[View]('groupBy, 'groupingColumns) }

  def groupByImpl[S : Type](df: Expr[StructDataFrame[S]])(using Quotes): Expr[GroupBy[?]] =
    import quotes.reflect.asTerm
    val viewExpr = StructSchemaView.schemaViewExpr[StructDataFrame[S]]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{ GroupBy[v](${ viewExpr }.asInstanceOf[v], ${ df }.untyped) }

  def applyImpl[View <: SchemaView : Type](groupBy: Expr[GroupBy[View]], groupingColumns: Expr[Seq[View ?=> NamedColumns[?]]])(using Quotes): Expr[GroupedDataFrame[View]] =
    import quotes.reflect.*

    val columnValuesWithTypes = groupingColumns match
      case Varargs(colExprs) =>
        colExprs.map { arg =>
          val reduced = Term.betaReduce('{$arg(using ${ groupBy }.view)}.asTerm).get
          reduced.asExpr match
            case '{ $value: NamedColumns[schema] } => ('{ ${ value }.underlyingColumns }, Type.of[schema])
        }

    val columnsValues = columnValuesWithTypes.map(_._1)
    val columnsTypes = columnValuesWithTypes.map(_._2)

    val groupedSchemaTpe = FrameSchema.schemaTypeFromColumnsTypes(columnsTypes)
    groupedSchemaTpe match
      case '[TupleSubtype[groupingKeys]] =>
        val groupedViewExpr = StructSchemaView.schemaViewExpr[StructDataFrame[groupingKeys]]

        groupedViewExpr.asTerm.tpe.asType match
          case '[SchemaView.Subtype[groupedView]] =>
            '{
              val groupingCols = ${ Expr.ofSeq(columnsValues) }.flatten
              new GroupedDataFrame[View]:
                type GroupingKeys = groupingKeys
                type GroupedView = groupedView
                def underlying = ${ groupBy }.underlying.groupBy(groupingCols*)
                def fullView = ${ groupBy }.view
                def groupedView = ${ groupedViewExpr }.asInstanceOf[GroupedView]
            }

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
      transparent inline def agg(inline columns: (Agg { type View = FullView }, GroupView) ?=> NamedColumns[?]*): StructDataFrame[?] =
        ${ aggImpl[FullView, GroupKeys, GroupView]('gdf, 'columns) }


  def aggImpl[FullView <: SchemaView : Type, GroupingKeys <: Tuple : Type, GroupView <: SchemaView : Type](
    gdf: Expr[GroupedDataFrame[FullView] { type GroupedView = GroupView }],
    columns: Expr[Seq[(Agg { type View = FullView }, GroupView) ?=> NamedColumns[?]]]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*

    val aggWrapper = '{
      new Agg:
        type View = FullView
        val view = ${ gdf }.fullView
    }

    val columnValuesWithTypes = columns match
      case Varargs(colExprs) =>
        colExprs.map { arg =>
          val reduced = Term.betaReduce('{$arg(using ${ aggWrapper }, ${ gdf }.groupedView)}.asTerm).get
          reduced.asExpr match
            case '{ $value: NamedColumns[schema] } => ('{ ${ value }.underlyingColumns }, Type.of[schema])
        }

    val columnsValues = columnValuesWithTypes.map(_._1)
    val columnsTypes = columnValuesWithTypes.map(_._2)

    val schemaTpe = FrameSchema.schemaTypeFromColumnsTypes(columnsTypes)
    schemaTpe match
      case '[s] =>
        '{
          // TODO assert cols is not empty
          val cols = ${ Expr.ofSeq(columnsValues) }.flatten
          StructDataFrame[FrameSchema.Merge[GroupingKeys, s]](
            ${ gdf }.underlying.agg(cols.head, cols.tail*)
          )
        }

trait Agg:
  type View <: SchemaView
  def view: View
