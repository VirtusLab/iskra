package org.virtuslab.iskra

import org.apache.spark.sql.{functions => f, Column => UntypedColumn}
import org.virtuslab.iskra.types.{Coerce, DataType, BooleanOptLike}

object When:
  class WhenColumn[T <: DataType](untyped: UntypedColumn) extends Col[DataType.AsNullable[T]](untyped):
    def when[U <: DataType](condition: Col[BooleanOptLike], value: Col[U])(using coerce: Coerce[T, U]): WhenColumn[coerce.Coerced] =
      WhenColumn(this.untyped.when(condition.untyped, value.untyped))
    def otherwise[U <: DataType](value: Col[U])(using coerce: Coerce[T, U]): Col[coerce.Coerced] =
      Col(this.untyped.otherwise(value.untyped))

  def when[T <: DataType](condition: Col[BooleanOptLike], value: Col[T]): WhenColumn[T] =
    WhenColumn(f.when(condition.untyped, value.untyped))
