package org.virtuslab.iskra

import scala.quoted.*
import MacroHelpers.TupleSubtype

class WithColumns[Schema, View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object WithColumns:
  given dataFrameWithColumnsOps: {} with
    extension [Schema, DF <: StructDataFrame[Schema]](df: DF)
      transparent inline def withColumns: WithColumns[Schema, ?] = ${ withColumnsImpl[Schema, DF]('{df}) }

  def withColumnsImpl[Schema : Type, DF <: StructDataFrame[Schema] : Type](df: Expr[DF])(using Quotes): Expr[WithColumns[Schema, ?]] =
    import quotes.reflect.asTerm
    val viewExpr = StructSchemaView.schemaViewExpr[DF]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new WithColumns[Schema, v](
            view = ${ viewExpr }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  given withColumnsApply: {} with
    extension [Schema <: Tuple, View <: SchemaView](withColumns: WithColumns[Schema, View])
      transparent inline def apply[C <: NamedColumns](columns: View ?=> C): StructDataFrame[?] =
        ${ applyImpl[Schema, View, C]('withColumns, 'columns) }

  private def applyImpl[Schema <: Tuple : Type, View <: SchemaView : Type, C : Type](
    withColumns: Expr[WithColumns[Schema, View]],
    columns: Expr[View ?=> C]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*

    Expr.summon[CollectColumns[C]] match
      case Some(collectColumns) =>
        collectColumns match
          case '{ $cc: CollectColumns[?] { type CollectedColumns = collectedColumns } } =>
            Type.of[collectedColumns] match
              case '[TupleSubtype[collectedCols]] =>
                '{
                  val cols = 
                    org.apache.spark.sql.functions.col("*") +: ${ cc }.underlyingColumns(${ columns }(using ${ withColumns }.view))
                  val withColumnsAppended =                    
                    ${ withColumns }.underlying.select(cols*)
                  StructDataFrame[Tuple.Concat[Schema, collectedCols]](withColumnsAppended)
                }
      case None =>
        throw CollectColumns.CannotCollectColumns(Type.show[C])
