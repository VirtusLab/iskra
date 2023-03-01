package org.virtuslab.iskra

import scala.quoted.*
import MacroHelpers.TupleSubtype

class WithColumn[Schema, View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object WithColumn:
  given dataFrameWithColumnOps: {} with
    extension [Schema, DF <: StructDataFrame[Schema]](df: DF)
      transparent inline def withColumn: WithColumn[Schema, ?] = ${ withColumnImpl[Schema, DF]('{df}) }

  def withColumnImpl[Schema : Type, DF <: StructDataFrame[Schema] : Type](df: Expr[DF])(using Quotes): Expr[WithColumn[Schema, ?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[DF]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new WithColumn[Schema, v](
            view = ${ viewExpr }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  given withColumnApply: {} with
    extension [Schema, View <: SchemaView](withColumn: WithColumn[Schema, View])
      transparent inline def apply[N <: Name, Col](name: N, column: View ?=> Col): StructDataFrame[?] =
        ${ applyImpl[Schema, View, N, Col]('withColumn, 'name, 'column) }

  def applyImpl[Schema : Type, View <: SchemaView : Type, N <: Name : Type, Col : Type](
    withColumn: Expr[WithColumn[Schema, View]],
    name: Expr[String],
    column: Expr[View ?=> Col]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*
    Type.of[Col] match
      case '[Column[colType]] =>
        '{
          type OutSchema = FrameSchema.Merge[Schema, N := colType]
          val col = ${ column }(using ${ withColumn }.view).asInstanceOf[Column[?]].untyped
          StructDataFrame[OutSchema](
            ${ withColumn }.underlying.withColumn(${name}, col)
          )
        }

      case '[t] =>
        val errorMsg = s"""The `column` parameter of `withColumn` should be of a column type but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(withColumn))
