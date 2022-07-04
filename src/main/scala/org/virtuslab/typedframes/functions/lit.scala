package org.virtuslab.typedframes.functions

import org.apache.spark.sql
import org.virtuslab.typedframes.TypedColumn
import org.virtuslab.typedframes.types.*

def lit[A](value: A)(using l: Lit[A]): TypedColumn[l.ColumnType] = TypedColumn(sql.functions.lit(value))

trait Lit[А]:
  type ColumnType <: DataType

object Lit:
  given Lit[String] with
    type ColumnType = StringType

  given Lit[Int] with
    type ColumnType = IntegerType

  given Lit[Boolean] with
    type ColumnType = BooleanType