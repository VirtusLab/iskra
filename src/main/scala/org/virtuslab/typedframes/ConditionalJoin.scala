package org.virtuslab.typedframes

import scala.quoted.*
import org.virtuslab.typedframes.types.{BooleanType, StructType}

abstract class ConditionalJoiner[DF1 <: TypedDataFrame[?], DF2 <: TypedDataFrame[?]](
  left: DF1, right: DF2, joinType: JoinType
):
  type MergedSchema <: StructType
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

    left.untyped.join(right.untyped, condition, sparkJoinType).withSchema[MergedSchema]

object ConditionalJoiner:
  def make[DF1 <: TypedDataFrame[?] : Type, DF2 <: TypedDataFrame[?] : Type](
    left: Expr[DF1], right: Expr[DF2], joinType: Expr[JoinType]
  )(using Quotes): Expr[ConditionalJoiner[DF1, DF2]] =
    import quotes.reflect.*
    mergeSchemaTypes(Type.of[DF1], Type.of[DF2]) match
      case '[s] =>
        val viewExpr = SelectionView.selectionViewExpr[TypedDataFrame[s & StructType]] // TODO: Handle aliases
        viewExpr.asTerm.tpe.asType match
          case '[v] =>
            '{
              new ConditionalJoiner($left, $right, $joinType) {
                type MergedSchema = s
                type MergedView = v
                val mergedView: MergedView = (${ viewExpr }).asInstanceOf[v]
              }.asInstanceOf[ConditionalJoiner[DF1, DF2] { type MergedSchema = s; type MergedView = v }]
            }

  def mergeSchemaTypes(df1: Type[?], df2: Type[?])(using Quotes): Type[?] =
    df1 match
      case '[TypedDataFrame[s1]] =>
        df2 match
          case '[TypedDataFrame[s2]] =>
            Type.of[StructType.Merge[s1, s2]]
