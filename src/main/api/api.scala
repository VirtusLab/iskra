package org.virtuslab.typedframes
package api

export DataFrameBuilders.toTypedDF
export types.{
  DataType,
  BooleanType,
  BooleanOptType,
  StringType,
  StringOptType,
  ByteType,
  ByteOptType,
  ShortType,
  ShortOptType,
  IntegerType,
  IntegerOptType,
  LongType,
  LongOptType,
  FloatType,
  FloatOptType,
  DoubleType,
  DoubleOptType,
  StructType,
  StructOptType
}
export UntypedOps.typed
export org.virtuslab.typedframes.$
export org.virtuslab.typedframes.{Column, DataFrame, UntypedColumn, UntypedDataFrame}

object functions:
  export org.virtuslab.typedframes.functions.{lit, avg, sum}

export org.apache.spark.sql.SparkSession
