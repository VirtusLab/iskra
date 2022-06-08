package org.virtuslab.typedframes

import types.*
import Internals.Name
import org.apache.spark.sql.functions.*

import org.virtuslab.typedframes.{TypedColumn as Col}

extension (col1: Col[IntegerType])
  inline def +(col2: Col[IntegerType]): Col[IntegerType] = (col1.untyped + col2.untyped).typed[IntegerType]
  inline def <(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped < col2.untyped).typed[BooleanType]
  inline def <=(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped <= col2.untyped).typed[BooleanType]
  inline def >(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped > col2.untyped).typed[BooleanType]
  inline def >=(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped >= col2.untyped).typed[BooleanType]

extension (col1: Col[StringType])
  inline def ++(col2: Col[StringType]): Col[StringType] = concat(col1.untyped, col2.untyped).typed[StringType]

extension [A <: DataType](col1: Col[A])
  inline def ===(col2: Col[A]): Col[BooleanType] = (col1.untyped === col2.untyped).typed[BooleanType]


extension (str: String)
  def asColumn: TypedColumn[StringType] = TypedColumn[StringType](lit(str))

// More operations can be added easily
