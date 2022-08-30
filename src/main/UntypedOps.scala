package org.virtuslab.iskra

import scala.quoted.*
import types.{DataType, StructType}

object UntypedOps:
  extension (untyped: UntypedColumn)
    def typed[A <: DataType] = Column[A](untyped)

  extension (df: UntypedDataFrame)
    transparent inline def typed[A](using encoder: DataType.StructEncoder[A]): DataFrame[?] = ${ typedDataFrameImpl('df, 'encoder) } // TODO: Check schema at runtime? Check if names of columns match?

  private def typedDataFrameImpl[A : Type](df: Expr[UntypedDataFrame], encoder: Expr[DataType.StructEncoder[A]])(using Quotes) =
    encoder match
      case '{ ${e}: DataType.Encoder.Aux[tpe, StructType[t]] } =>
        '{ DataFrame[t](${ df }) }
