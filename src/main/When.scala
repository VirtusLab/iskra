package org.virtuslab.iskra

import org.apache.spark.sql.{functions => f, Column => UntypedColumn}
import org.virtuslab.iskra.types.{Coerce, DataType, BooleanOptType}

object When:
  class WhenColumn[T <: DataType](untyped: UntypedColumn) extends Column[DataType.Nullable[T]](untyped):
    def when[U <: DataType](condition: Column[BooleanOptType], value: Column[U])(using coerce: Coerce[T, U]): WhenColumn[coerce.Coerced] =
      WhenColumn(this.untyped.when(condition.untyped, value.untyped))
    def otherwise[U <: DataType](value: Column[U])(using coerce: Coerce[T, U]): Column[coerce.Coerced] =
      Column(this.untyped.otherwise(value.untyped))

  def when[T <: DataType](condition: Column[BooleanOptType], value: Column[T]): WhenColumn[T] =
    WhenColumn(f.when(condition.untyped, value.untyped))
