package org.virtuslab.typedframes.functions

import org.apache.spark.sql
import org.virtuslab.typedframes.Agg
import org.virtuslab.typedframes.{Column as Col}
import org.virtuslab.typedframes.Column.untypedColumnOps
import org.virtuslab.typedframes.types.*
import org.virtuslab.typedframes.types.DataType.{NotNull, NumericOptType}
import scala.util.NotGiven

def avg(using agg: Agg)(colFun: agg.View ?=> Col[NumericOptType]): Col[DoubleOptType] =
  sql.functions.avg(colFun(using agg.view).untyped).typed

def sum[T <: NumericOptType](using agg: Agg, isNullable: NotGiven[T <:< NotNull])(colFun: agg.View ?=> Col[T]): Col[T] =
  sql.functions.avg(colFun(using agg.view).untyped).typed