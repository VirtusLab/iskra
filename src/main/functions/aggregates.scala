package org.virtuslab.iskra.functions

import org.apache.spark.sql
import org.virtuslab.iskra.Agg
import org.virtuslab.iskra.Column
import org.virtuslab.iskra.UntypedOps.typed
import org.virtuslab.iskra.types.*
import org.virtuslab.iskra.types.DataType.{NumericOptType, Nullable}

class Sum[A <: Agg](val agg: A):
  def apply[T <: NumericOptType](column: agg.View ?=> Column[T]): Column[Nullable[T]] =
    sql.functions.sum(column(using agg.view).untyped).typed

class Max[A <: Agg](val agg: A):
  def apply[T <: NumericOptType](column: agg.View ?=> Column[T]): Column[Nullable[T]] =
    sql.functions.max(column(using agg.view).untyped).typed

class Min[A <: Agg](val agg: A):
  def apply[T <: NumericOptType](column: agg.View ?=> Column[T]): Column[Nullable[T]] =
    sql.functions.min(column(using agg.view).untyped).typed

class Avg[A <: Agg](val agg: A):
  def apply(column: agg.View ?=> Column[NumericOptType]): Column[DoubleOptType] =
    sql.functions.avg(column(using agg.view).untyped).typed

object Aggregates:
  def sum(using agg: Agg): Sum[agg.type] = new Sum(agg)
  def max(using agg: Agg): Max[agg.type] = new Max(agg)
  def min(using agg: Agg): Min[agg.type] = new Min(agg)
  def avg(using agg: Agg): Avg[agg.type] = new Avg(agg)
