package org.virtuslab.typedframes.types

import org.virtuslab.typedframes.{TypedColumn as Col}
import org.virtuslab.typedframes.TypedColumn.untypedColumnOps

trait BooleanType extends DataType

object BooleanType:
  given booleanColumnOps: {} with
    extension (col1: Col[BooleanType])
      inline def ===(col2: Col[BooleanType]): Col[BooleanType] = (col1.untyped === col2.untyped).typed
      inline def &&(col2: Col[BooleanType]): Col[BooleanType] = (col1.untyped && col2.untyped).typed
      inline def ||(col2: Col[BooleanType]): Col[BooleanType] = (col1.untyped || col2.untyped).typed
