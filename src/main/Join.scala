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
  type Left <: DataFrame[?]
  type Right <: DataFrame[?]

object Join:
  given dataFrameJoinOps: {} with
    extension [LeftDF <: DataFrame[?]](inline leftDF: LeftDF)
      transparent inline def join[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Inner.type] =
        ${ joinImpl[JoinType.Inner.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def innerJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Inner.type] =
        ${ joinImpl[JoinType.Inner.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def leftJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Left.type] =
        ${ joinImpl[JoinType.Left.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def rightJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Right.type] =
        ${ joinImpl[JoinType.Right.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def fullJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Full.type] =
        ${ joinImpl[JoinType.Full.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def semiJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Semi.type] =
        ${ joinImpl[JoinType.Semi.type, LeftDF, RightDF]('leftDF, 'rightDF) }
      transparent inline def antiJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): Join[JoinType.Anti.type] =
        ${ joinImpl[JoinType.Anti.type, LeftDF, RightDF]('leftDF, 'rightDF) }

      transparent inline def crossJoin[RightDF <: DataFrame[?]](inline rightDF: RightDF): DataFrame[?] =
        ${ crossJoinImpl[LeftDF, RightDF]('leftDF, 'rightDF) }
  end dataFrameJoinOps

  def joinImpl[T <: JoinType : Type, LeftDF <: DataFrame[?] : Type, RightDF <: DataFrame[?] : Type](
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

  def crossJoinImpl[LeftDF <: DataFrame[?] : Type, RightDF <: DataFrame[?] : Type](
    leftDF: Expr[LeftDF], rightDF: Expr[RightDF]
  )(using Quotes): Expr[DataFrame[?]] =
    Aliasing.autoAliasImpl(leftDF) match
      case '{ $left: DataFrame[l] } =>
        Aliasing.autoAliasImpl(rightDF) match
          case '{ $right: DataFrame[r] } =>
            '{
              val joined = ${left}.untyped.crossJoin(${right}.untyped)
              DataFrame[FrameSchema.Merge[l, r]](joined)
            }

  export JoinOnCondition.joinOnConditionOps
