package org.virtuslab.typedframes

import types.*
import Internals.Name
import org.apache.spark.sql.functions.*

extension (col1: TypedColumn[IntegerType])
  inline def +(col2: TypedColumn[IntegerType]) = TypedColumn[IntegerType](col1.underlying + col2.underlying)

extension (col1: TypedColumn[StringType])
  inline def ++(col2: TypedColumn[StringType]) = TypedColumn[StringType](concat(col1.underlying, col2.underlying))

extension (str: String)
  def asColumn: TypedColumn[StringType] = TypedColumn[StringType](lit(str))

// More operations can be added easily
