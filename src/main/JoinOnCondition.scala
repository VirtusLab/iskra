package org.virtuslab.typedframes

import scala.quoted.*
import org.virtuslab.typedframes.types.BooleanOptType

abstract class JoinOnCondition[MergedSchema, MergedView <: SchemaView](
  val left: UntypedDataFrame, val right: UntypedDataFrame, val joinType: JoinType
):
  val mergedView: MergedView

object JoinOnCondition:
  given joinOnConditionOps: {} with
    extension [Schema, View <: SchemaView] (joc: JoinOnCondition[Schema, View])
      inline def apply[ConditionColumn](conditionColumn: View ?=> ConditionColumn): DataFrame[Schema] =
        ${ applyImpl[Schema, View, ConditionColumn]('joc, 'conditionColumn) }
  
  def applyImpl[Schema : Type, View <: SchemaView : Type, ConditionColumn : Type](
    joc: Expr[JoinOnCondition[Schema, View]],
    conditionColumn: Expr[View ?=> ConditionColumn]
  )(using Quotes): Expr[DataFrame[Schema]] =
    import quotes.reflect.*
    Type.of[ConditionColumn] match
      case '[Column[BooleanOptType]] =>
        '{
          val condition = ${ conditionColumn }(using ${ joc }.mergedView).asInstanceOf[Column[BooleanOptType]].untyped

          val sparkJoinType = ${ joc }.joinType match
            case JoinType.Inner => "inner"
            case JoinType.Left => "left"
            case JoinType.Right => "right"
            case JoinType.Outer => "outer"

          val joined = ${ joc }.left.join(${ joc }.right, condition, sparkJoinType)
          DataFrame[Schema](joined)
        }

      case '[t] =>
        val errorMsg = s"""The parameter of `on` should be a (potentially nullable) boolean column (${Type.show[Column[BooleanOptType]]}) but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(joc))

  def make[DF1 <: DataFrame[?] : Type, DF2 <: DataFrame[?] : Type](
    left: Expr[UntypedDataFrame], right: Expr[UntypedDataFrame], joinType: Expr[JoinType]
  )(using Quotes): Expr[JoinOnCondition[?, ?]] =
    import quotes.reflect.*
    mergeSchemaTypes(Type.of[DF1], Type.of[DF2]) match
      case '[s] if FrameSchema.isValidType(Type.of[s]) =>
        val viewExpr = SchemaView.schemaViewExpr[DataFrame[s]]
        viewExpr.asTerm.tpe.asType match
          case '[SchemaView.Subtype[v]] =>
            '{
              new JoinOnCondition[s, v](${left}, ${right}, ${joinType}) {
                val mergedView: v = (${ viewExpr }).asInstanceOf[v]
              }
            }

  def mergeSchemaTypes(df1: Type[?], df2: Type[?])(using Quotes): Type[?] =
    df1 match
      case '[DataFrame[s1]] =>
        df2 match
          case '[DataFrame[s2]] =>
            Type.of[FrameSchema.Merge[s1, s2]]
