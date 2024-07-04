package org.virtuslab.iskra

import scala.quoted.*
import types.{DataType, Encoder, StructType, StructEncoder}

object UntypedOps:
  extension (untyped: UntypedColumn)
    def typed[A <: DataType] = Col[A](untyped)

  extension (df: UntypedDataFrame)
    transparent inline def typed[A](using encoder: StructEncoder[A]): ClassDataFrame[?] = ${ typedDataFrameImpl('df, 'encoder) } // TODO: Check schema at runtime? Check if names of columns match?

  private def typedDataFrameImpl[A : Type](df: Expr[UntypedDataFrame], encoder: Expr[StructEncoder[A]])(using Quotes) =
    encoder match
      case '{ ${e}: Encoder.Aux[tpe, StructType[t]] } =>
        '{ ClassDataFrame[A](${ df }) }
