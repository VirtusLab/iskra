package org.virtuslab.iskra

import scala.quoted.*
import types.{DataType, Encoder, struct, StructEncoder, StructNotNull}

object UntypedOps:
  extension (untyped: UntypedColumn)
    def unsafeTyped[R <: Column]: R = new ColumnImpl(untyped).asInstanceOf[R]

  extension (df: UntypedDataFrame)
    transparent inline def unsafeTyped[A](using encoder: StructEncoder[A]): ClassDataFrame[?] = ${ typedDataFrameImpl('df, 'encoder) } // TODO: Check schema at runtime? Check if names of columns match?

  private def typedDataFrameImpl[A : Type](df: Expr[UntypedDataFrame], encoder: Expr[StructEncoder[A]])(using Quotes) =
    encoder match
      case '{ ${e}: Encoder.Aux[tpe, StructNotNull[t]] } =>
        '{ ClassDataFrame[A](${ df }) }
