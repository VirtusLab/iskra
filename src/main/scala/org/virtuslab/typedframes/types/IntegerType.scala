package org.virtuslab.typedframes.types

import org.virtuslab.typedframes.{TypedColumn as Col}
import org.virtuslab.typedframes.TypedColumn.untypedColumnOps

trait IntegerType extends DataType

object IntegerType:
  given integerColumnOps: {} with
    extension (col1: Col[IntegerType])
      inline def +(col2: Col[IntegerType]): Col[IntegerType] = (col1.untyped + col2.untyped).typed
      inline def <(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped < col2.untyped).typed
      inline def <=(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped <= col2.untyped).typed
      inline def >(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped > col2.untyped).typed
      inline def >=(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped >= col2.untyped).typed
      inline def ===(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped === col2.untyped).typed