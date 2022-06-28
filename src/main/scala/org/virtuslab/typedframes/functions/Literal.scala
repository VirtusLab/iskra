package org.virtuslab.typedframes.functions

import org.apache.spark.sql
import org.virtuslab.typedframes.TypedColumn
import org.virtuslab.typedframes.types.*

def lit[A](value: A)(using l: Literal[A]): TypedColumn[l.ColumnType] = TypedColumn(sql.functions.lit(value))

// TODO: Repackage so that import functions.* doesn't pollute the namespace with implementation details

trait Literal[А]:
  type ColumnType <: DataType

object Literal:
  given Literal[String] with
    type ColumnType = StringType

  given Literal[Int] with
    type ColumnType = IntegerType

  given Literal[Boolean] with
    type ColumnType = BooleanType