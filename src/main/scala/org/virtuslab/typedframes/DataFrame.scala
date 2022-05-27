package org.virtuslab.typedframes

import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, Encoder, SparkSession }
import types.{DataType, StructType}

object TypedDataFrameOpaqueScope:
  opaque type TypedDataFrame[+S <: StructType] = UntypedDataFrame
  extension (inline df: UntypedDataFrame)
    inline def typed[A](using encoder: DataType.StructEncoder[A]): TypedDataFrame[encoder.Encoded] = df // TODO: Check schema at runtime? Check if names of columns match?
    inline def withSchema[S <: StructType]: TypedDataFrame[S] = df // TODO? make it private[typedframes]

  extension [S <: StructType](tdf: TypedDataFrame[S])
    inline def untyped: UntypedDataFrame = tdf

export TypedDataFrameOpaqueScope.*