package org.virtuslab.typedframes

import scala.quoted.*

enum JoinType:
  case Inner
  case Left
  case Right
  case Outer
  // TODO: Add other join types (treating Cross join separately?)

class Join[DF1 <: DataFrame[?], DF2 <: DataFrame[?]](left: UntypedDataFrame, right: UntypedDataFrame, joinType: JoinType):
  transparent inline def on: JoinOnCondition[?, ?] = ${ JoinOnCondition.make[DF1, DF2]('left, 'right, 'joinType) }

object Join:
  given dataFrameJoinOps: {} with
    extension [DF1 <: DataFrame[?], DF2 <: DataFrame[?]](inline df1: DF1)
      transparent inline def join(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Inner}) }
      transparent inline def innerJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Inner}) }

      // TODO: assure nullability of joined columns
      // transparent inline def leftJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Left}) }
      // transparent inline def rightJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Right}) }
      // transparent inline def outerJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Outer}) }

  def joinImpl[DF1 <: DataFrame[?] : Type, DF2 <: DataFrame[?] : Type](using Quotes)(
    df1: Expr[DF1], df2: Expr[DF2], joinType: Expr[JoinType]
  ) = 
    import quotes.reflect.*

    Aliasing.autoAliasTypeImpl(df1).asType match
      case '[DataFrame.Subtype[dft1]] =>
        Aliasing.autoAliasTypeImpl(df2).asType match
          case '[DataFrame.Subtype[dft2]] =>
            '{
              new Join[dft1, dft2](
                ${ Aliasing.autoAliasImpl[DF1](df1) },
                ${ Aliasing.autoAliasImpl[DF2](df2) },
                ${ joinType }
              )
            }
