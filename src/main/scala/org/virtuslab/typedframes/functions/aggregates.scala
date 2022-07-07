package org.virtuslab.typedframes.functions

import org.apache.spark.sql
import org.virtuslab.typedframes.Agg
import org.virtuslab.typedframes.{Column as Col}
import org.virtuslab.typedframes.Column.untypedColumnOps
import org.virtuslab.typedframes.types.*

def avg[T <: IntegerType | FloatType | DoubleType](using agg: Agg)(colFun: agg.View ?=> Col[T]): Col[DoubleType] =
  sql.functions.avg(colFun(using agg.view).untyped).typed
def sum[T <: IntegerType | FloatType | DoubleType](using agg: Agg)(colFun: agg.View ?=> Col[T]): Col[T] =
  sql.functions.avg(colFun(using agg.view).untyped).typed