package org.virtuslab.typedframes

import types.*
import Internals.Name
import org.apache.spark.sql.functions.*

import org.virtuslab.typedframes.{TypedColumn as Col}

given integerColumnOps: {} with
  extension (col1: Col[IntegerType])
    inline def +(col2: Col[IntegerType]): Col[IntegerType] = (col1.untyped + col2.untyped).typed
    inline def <(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped < col2.untyped).typed
    inline def <=(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped <= col2.untyped).typed
    inline def >(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped > col2.untyped).typed
    inline def >=(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped >= col2.untyped).typed
    inline def ===(col2: Col[IntegerType]): Col[BooleanType] = (col1.untyped === col2.untyped).typed

given stringColumnOps: {} with
  extension (col1: Col[StringType])
    inline def ++(col2: Col[StringType]): Col[StringType] = concat(col1.untyped, col2.untyped).typed[StringType]
    inline def ===(col2: Col[StringType]): Col[BooleanType] = (col1.untyped === col2.untyped).typed[BooleanType]

given stringLiteralColumnOps: {} with
  extension (s: String)
    def asColumn: Col[StringType] = lit(s).typed

given integerLiteralColumnOps: {} with
  extension (i: Int)
    def asColumn: Col[IntegerType] = lit(i).typed

given booleanLiteralColumnOps: {} with
  extension (b: Boolean)
    def asColumn: Col[BooleanType] = lit(b).typed

// More operations can be added easily
