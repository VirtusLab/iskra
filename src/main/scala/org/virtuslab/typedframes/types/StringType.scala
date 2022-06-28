package org.virtuslab.typedframes.types

import org.apache.spark.sql.functions.concat
import org.virtuslab.typedframes.{TypedColumn as Col}
import org.virtuslab.typedframes.TypedColumn.untypedColumnOps

trait StringType extends DataType

object StringType:
  given stringColumnOps: {} with
    extension (col1: Col[StringType])
      inline def ++(col2: Col[StringType]): Col[StringType] = concat(col1.untyped, col2.untyped).typed
      inline def ===(col2: Col[StringType]): Col[BooleanType] = (col1.untyped === col2.untyped).typed