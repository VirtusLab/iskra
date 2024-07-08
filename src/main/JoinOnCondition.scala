package org.virtuslab.iskra

import scala.language.implicitConversions

import scala.quoted.*
import org.virtuslab.iskra.types.BooleanOptType

trait OnConditionJoiner[Join <: JoinType, Left, Right]

trait JoinOnCondition[Join <: JoinType, Left <: StructDataFrame[?], Right <: StructDataFrame[?]]:
  type JoiningView <: SchemaView
  type JoinedSchema
  def joiningView: JoiningView

object JoinOnCondition:
  private type LeftWithRight[Left, Right] = FrameSchema.Merge[Left, Right]
  private type LeftWithOptionalRight[Left, Right] = FrameSchema.Merge[Left, FrameSchema.NullableSchema[Right]]
  private type OptionalLeftWithRight[Left, Right] = FrameSchema.Merge[FrameSchema.NullableSchema[Left], Right]
  private type OptionalLeftWithOptionalRight[Left, Right] = FrameSchema.Merge[FrameSchema.NullableSchema[Left], FrameSchema.NullableSchema[Right]]
  private type OnlyLeft[Left, Right] = Left

  transparent inline given inner[Left <: StructDataFrame[?], Right <: StructDataFrame[?]]: JoinOnCondition[JoinType.Inner.type, Left, Right] =
    ${ joinOnConditionImpl[JoinType.Inner.type, LeftWithRight, Left, Right] }

  transparent inline given left[Left <: StructDataFrame[?], Right <: StructDataFrame[?]]: JoinOnCondition[JoinType.Left.type, Left, Right] =
    ${ joinOnConditionImpl[JoinType.Left.type, LeftWithOptionalRight, Left, Right] }

  transparent inline given right[Left <: StructDataFrame[?], Right <: StructDataFrame[?]]: JoinOnCondition[JoinType.Right.type, Left, Right] =
    ${ joinOnConditionImpl[JoinType.Right.type, OptionalLeftWithRight, Left, Right] }

  transparent inline given full[Left <: StructDataFrame[?], Right <: StructDataFrame[?]]: JoinOnCondition[JoinType.Full.type, Left, Right] =
    ${ joinOnConditionImpl[JoinType.Full.type, [S1, S2] =>> FrameSchema.Merge[FrameSchema.NullableSchema[S1], FrameSchema.NullableSchema[S2]], Left, Right] }

  transparent inline given semi[Left <: StructDataFrame[?], Right <: StructDataFrame[?]]: JoinOnCondition[JoinType.Semi.type, Left, Right] =
    ${ joinOnConditionImpl[JoinType.Semi.type, OnlyLeft, Left, Right] }

  transparent inline given anti[Left <: StructDataFrame[?], Right <: StructDataFrame[?]]: JoinOnCondition[JoinType.Anti.type, Left, Right] =
    ${ joinOnConditionImpl[JoinType.Anti.type, OnlyLeft, Left, Right] }


  def joinOnConditionImpl[Join <: JoinType : Type, MergeSchemas[S1, S2] : Type, Left <: StructDataFrame[?] : Type, Right <: StructDataFrame[?] : Type](using Quotes) =
    import quotes.reflect.*
    
    Type.of[Left] match
      case '[StructDataFrame[s1]] =>
        Type.of[Right] match
          case '[StructDataFrame[s2]] =>
            Type.of[FrameSchema.Merge[s1, s2]] match
              case '[viewSchema] if FrameSchema.isValidType(Type.of[viewSchema]) =>
                Type.of[MergeSchemas[s1, s2]] match
                  case '[joinedSchema] if FrameSchema.isValidType(Type.of[joinedSchema]) =>
                    val viewExpr = StructSchemaView.schemaViewExpr[StructDataFrame[viewSchema]]
                    viewExpr.asTerm.tpe.asType match
                      case '[SchemaView.Subtype[v]] =>
                        '{
                          new JoinOnCondition[Join, Left, Right] {
                            override type JoinedSchema = joinedSchema
                            override type JoiningView = v
                            val joiningView: JoiningView = (${ viewExpr }).asInstanceOf[v]
                          }
                        }

  implicit def joinOnConditionOps[T <: JoinType](join: Join[T])(using joc: JoinOnCondition[T, join.Left, join.Right]): JoinOnConditionOps[T, joc.JoiningView, joc.JoinedSchema] =
    new JoinOnConditionOps[T, joc.JoiningView, joc.JoinedSchema](join, joc.joiningView)

  class JoinOnConditionOps[T <: JoinType, JoiningView <: SchemaView, JoinedSchema](join: Join[T], joiningView: JoiningView):
    inline def on[Condition](condition: JoiningView ?=> Condition): StructDataFrame[JoinedSchema] =      
      ${ joinOnImpl[T, JoiningView, JoinedSchema, Condition]('join, 'joiningView, 'condition) }

  def joinOnImpl[T <: JoinType : Type, JoiningView <: SchemaView : Type, JoinedSchema : Type, Condition : Type](
    join: Expr[Join[?]], joiningView: Expr[JoiningView], condition: Expr[JoiningView ?=> Condition]
  )(using Quotes) =
    import quotes.reflect.*

    '{ ${ condition }(using ${ joiningView }) } match
      case '{ $cond: Col[BooleanOptType] } =>
        '{
          val joined = ${ join }.left.join(${ join }.right, ${ cond }.untyped, JoinType.typeName[T])
          StructDataFrame[JoinedSchema](joined)
        }
      case '{ $cond: condType } =>
        val errorMsg = s"""The join condition of `on` has to be a (potentially nullable) boolean column but it has type: ${Type.show[condType]}"""
        // TODO: improve error position
        report.errorAndAbort(errorMsg)
