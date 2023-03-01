package org.virtuslab.iskra

import scala.quoted.*
import scala.compiletime.erasedValue

enum JoinType:
  case Inner
  case Left
  case Right
  case Full
  case Semi
  case Anti

object JoinType:
  inline def typeName[T <: JoinType] = inline erasedValue[T] match
    case _: Inner.type => "inner"
    case _: Left.type => "left"
    case _: Right.type => "right"
    case _: Full.type => "full"
    case _: Semi.type => "semi"
    case _: Anti.type => "anti"

trait Join[T <: JoinType](val left: UntypedDataFrame, val right: UntypedDataFrame):
  type Left <: StructDataFrame[?]
  type Right <: StructDataFrame[?]

object Join:
  given dataFrameJoinOps: {} with
    extension [LeftDF <: StructDataFrame[?]](inline leftDF: LeftDF)
      transparent inline def join[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Inner.type] =
        ${ joinImpl[JoinType.Inner.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def innerJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Inner.type] =
        ${ joinImpl[JoinType.Inner.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def leftJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Left.type] =
        ${ joinImpl[JoinType.Left.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def rightJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Right.type] =
        ${ joinImpl[JoinType.Right.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def fullJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Full.type] =
        ${ joinImpl[JoinType.Full.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def semiJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Semi.type] =
        ${ joinImpl[JoinType.Semi.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def antiJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): Join[JoinType.Anti.type] =
        ${ joinImpl[JoinType.Anti.type, LeftDF, RightDF]('leftDF, 'rightDF) }

      transparent inline def crossJoin[RightDF <: StructDataFrame[?]](inline rightDF: RightDF): StructDataFrame[?] =
        ${ crossJoinImpl[LeftDF, RightDF]('leftDF, 'rightDF) }
  end dataFrameJoinOps

  def joinImpl[T <: JoinType : Type, LeftDF <: StructDataFrame[?] : Type, RightDF <: StructDataFrame[?] : Type](
    leftDF: Expr[LeftDF], rightDF: Expr[RightDF]
  )(using Quotes) =
    Aliasing.autoAliasImpl(leftDF) match
      case '{ $left: l } =>
        Aliasing.autoAliasImpl(rightDF) match
          case '{ $right: r } =>
            '{
              (new Join[T](${left}.untyped, ${right}.untyped) { type Left = l; type Right = r })
                : Join[T]{ type Left = l; type Right = r }
            }

  def crossJoinImpl[LeftDF <: StructDataFrame[?] : Type, RightDF <: StructDataFrame[?] : Type](
    leftDF: Expr[LeftDF], rightDF: Expr[RightDF]
  )(using Quotes): Expr[StructDataFrame[?]] =
    Aliasing.autoAliasImpl(leftDF) match
      case '{ $left: StructDataFrame[l] } =>
        Aliasing.autoAliasImpl(rightDF) match
          case '{ $right: StructDataFrame[r] } =>
            '{
              val joined = ${left}.untyped.crossJoin(${right}.untyped)
              StructDataFrame[FrameSchema.Merge[l, r]](joined)
            }

  export JoinOnCondition.joinOnConditionOps
