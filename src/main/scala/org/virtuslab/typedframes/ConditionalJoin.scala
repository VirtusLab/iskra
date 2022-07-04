package org.virtuslab.typedframes

import scala.quoted.*
import org.virtuslab.typedframes.types.{BooleanType, StructType}

abstract class ConditionalJoin[DF1 <: DataFrame[?], DF2 <: DataFrame[?]](
  left: UntypedDataFrame, right: UntypedDataFrame, joinType: JoinType
):
  type MergedSchema <: FrameSchema
  type MergedView <: SchemaView
  val mergedView: MergedView
  def apply(conditionColumn: MergedView ?=> TypedColumn[BooleanType]): DataFrame[MergedSchema] =
    val condition = conditionColumn(using mergedView).untyped

    val sparkJoinType = joinType match
      case JoinType.Inner => "inner"
      case JoinType.Left => "left"
      case JoinType.Right => "right"
      case JoinType.Outer => "outer"

    val joined = left.join(right, condition, sparkJoinType)
    DataFrame[MergedSchema](joined)

object ConditionalJoin:
  def make[DF1 <: DataFrame[?] : Type, DF2 <: DataFrame[?] : Type](
    left: Expr[UntypedDataFrame], right: Expr[UntypedDataFrame], joinType: Expr[JoinType]
  )(using Quotes): Expr[ConditionalJoin[DF1, DF2]] =
    import quotes.reflect.*
    mergeSchemaTypes(Type.of[DF1], Type.of[DF2]) match
      case '[FrameSchema.Subtype[s]] =>
        val viewExpr = SchemaView.schemaViewExpr[DataFrame[s]]
        viewExpr.asTerm.tpe.asType match
          case '[SchemaView.Subtype[v]] =>
            '{
              new ConditionalJoin[DF1, DF2](${left}, ${right}, ${joinType}) {
                type MergedSchema = s
                type MergedView = v
                val mergedView: MergedView = (${ viewExpr }).asInstanceOf[v]
              }
            }

  def mergeSchemaTypes(df1: Type[?], df2: Type[?])(using Quotes): Type[?] =
    df1 match
      case '[DataFrame[s1]] =>
        df2 match
          case '[DataFrame[s2]] =>
            Type.of[FrameSchema.Merge[s1, s2]]
