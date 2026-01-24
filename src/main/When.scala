package org.virtuslab.iskra

import org.apache.spark.sql.{functions => f, Column => UntypedColumn}
import org.virtuslab.iskra.types.{Coerce, DataType, BooleanOptLike}

object When:
  opaque type WhenColumn[T <: DataType] <: Col[DataType.AsNullable[T]] = Col[DataType.AsNullable[T]]

  object WhenColumn:
    extension [T <: DataType](whenCol: WhenColumn[T])
      def when[U <: DataType](condition: Col[BooleanOptLike], value: Col[U])(using coerce: Coerce[T, U]): WhenColumn[coerce.Coerced] =
        Col(whenCol.untyped.when(condition.untyped, value.untyped))
      def otherwise[U <: DataType](value: Col[U])(using coerce: Coerce[T, U]): Col[coerce.Coerced] =
        Col(whenCol.untyped.otherwise(value.untyped))

  def when[T <: DataType](condition: Col[BooleanOptLike], value: Col[T]): WhenColumn[T] =
    Col(f.when(condition.untyped, value.untyped))
