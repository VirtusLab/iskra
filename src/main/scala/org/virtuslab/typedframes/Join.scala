package org.virtuslab.typedframes

import scala.quoted.*

import Internals.Name
import org.virtuslab.typedframes.types.StructType
import org.apache.spark.sql.{ DataFrame => UntypedDataFrame }

enum JoinType:
  case Inner
  case Left
  case Right
  case Outer
  // TODO: Add other join types (treating Cross join separately?)

class JoinOps[DF1 <: TypedDataFrame[FrameSchema], DF2 <: TypedDataFrame[FrameSchema]](left: UntypedDataFrame, right: UntypedDataFrame, joinType: JoinType):
  transparent inline def on = ${ ConditionalJoiner.make[DF1, DF2]('left, 'right, 'joinType) }

extension [DF1 <: TypedDataFrame[FrameSchema], DF2 <: TypedDataFrame[FrameSchema]](inline df1: DF1)
  transparent inline def join(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Inner}) }
  transparent inline def innerJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Inner}) }
  transparent inline def leftJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Left}) }
  transparent inline def rightJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Right}) }
  transparent inline def outerJoin(inline df2: DF2) = ${ joinImpl('{df1}, '{df2}, '{JoinType.Outer}) }

def joinImpl[DF1 <: TypedDataFrame[FrameSchema] : Type, DF2 <: TypedDataFrame[FrameSchema] : Type](using Quotes)(
  df1: Expr[DF1], df2: Expr[DF2], joinType: Expr[JoinType]
) = 
  import quotes.reflect.*

  TypedDataFrame.autoAliasTypeImpl(df1).asType match
    case '[TypedDataFrame.Subtype[dft1]] =>
      TypedDataFrame.autoAliasTypeImpl(df2).asType match
        case '[TypedDataFrame.Subtype[dft2]] =>
          '{
            new JoinOps[dft1, dft2](
              ${ TypedDataFrame.autoAliasImpl[DF1](df1) },
              ${ TypedDataFrame.autoAliasImpl[DF2](df2) },
              ${ joinType }
            )
          }
