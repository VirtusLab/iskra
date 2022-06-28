package org.virtuslab.typedframes

import scala.quoted.*
import org.virtuslab.typedframes.types.{BooleanType, StructType}
import org.apache.spark.sql.{DataFrame => UntypedDataFrame}

abstract class ConditionalJoiner[DF1 <: TypedDataFrame[?], DF2 <: TypedDataFrame[?]](
  left: UntypedDataFrame, right: UntypedDataFrame, joinType: JoinType
):
  type MergedSchema <: FrameSchema
  type MergedView <: SelectionView
  val mergedView: MergedView
  def apply(f: SelectCtx { type CtxOut = MergedView } ?=> TypedColumn[BooleanType]): TypedDataFrame[MergedSchema] =
    val ctx = new SelectCtx {
      type CtxOut = MergedView
      def ctxOut: MergedView = mergedView
    }
    val condition = f(using ctx).untyped

    val sparkJoinType = joinType match
      case JoinType.Inner => "inner"
      case JoinType.Left => "left"
      case JoinType.Right => "right"
      case JoinType.Outer => "outer"

    left.join(right, condition, sparkJoinType).withSchema[MergedSchema]

object ConditionalJoiner:
  def make[DF1 <: TypedDataFrame[?] : Type, DF2 <: TypedDataFrame[?] : Type](
    left: Expr[UntypedDataFrame], right: Expr[UntypedDataFrame], joinType: Expr[JoinType]
  )(using Quotes): Expr[ConditionalJoiner[DF1, DF2]] =
    import quotes.reflect.*
    mergeSchemaTypes(Type.of[DF1], Type.of[DF2]) match
      case '[FrameSchema.Subtype[s]] =>
        val viewExpr = SelectionView.selectionViewExpr[TypedDataFrame[s]]
        viewExpr.asTerm.tpe.asType match
          case '[SelectionView.Subtype[v]] =>
            '{
              new ConditionalJoiner[DF1, DF2](${left}, ${right}, ${joinType}) {
                type MergedSchema = s
                type MergedView = v
                val mergedView: MergedView = (${ viewExpr }).asInstanceOf[v]
              }
            }

  def mergeSchemaTypes(df1: Type[?], df2: Type[?])(using Quotes): Type[?] =
    df1 match
      case '[TypedDataFrame[s1]] =>
        df2 match
          case '[TypedDataFrame[s2]] =>
            Type.of[FrameSchema.Merge[s1, s2]]
