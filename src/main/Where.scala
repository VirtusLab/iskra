package org.virtuslab.iskra

import scala.quoted.*
import org.virtuslab.iskra.types.BooleanOptType

trait Where[Schema, View <: SchemaView]:
  val view: View
  def underlying: UntypedDataFrame

object Where:
  given dataFrameWhereOps: {} with
    extension [Schema](df: StructDataFrame[Schema])
      transparent inline def where: Where[Schema, ?] = ${ Where.whereImpl[Schema]('{df}) }

  def whereImpl[Schema : Type](df: Expr[StructDataFrame[Schema]])(using Quotes): Expr[Where[Schema, ?]] =
    import quotes.reflect.asTerm
    val viewExpr = StructSchemaView.schemaViewExpr[StructDataFrame[Schema]]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new Where[Schema, v] {
            val view = ${ viewExpr }.asInstanceOf[v]
            def underlying = ${ df }.untyped
          }
        }

  given whereApply: {} with
    extension [Schema, View <: SchemaView](where: Where[Schema, View])
      inline def apply[Condition](condition: View ?=> Condition): StructDataFrame[Schema] =
        ${ Where.applyImpl[Schema, View, Condition]('where, 'condition) }

  def applyImpl[Schema : Type, View <: SchemaView : Type, Condition : Type](
    where: Expr[Where[Schema, View]],
    condition: Expr[View ?=> Condition]
  )(using Quotes): Expr[StructDataFrame[Schema]] =
    import quotes.reflect.*

    '{ ${ condition }(using ${ where }.view) } match
      case '{ $cond: Col[BooleanOptType] } =>
        '{
          val filtered = ${ where }.underlying.where(${ cond }.untyped)
          StructDataFrame[Schema](filtered)
        }
      case '{ $cond: condType } =>
        val errorMsg = s"""The filtering condition of `where` has to be a (potentially nullable) boolean column but it has type: ${Type.show[condType]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(where))
