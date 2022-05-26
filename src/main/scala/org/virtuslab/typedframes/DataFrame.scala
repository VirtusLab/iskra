package org.virtuslab.typedframes

import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, Encoder, SparkSession /* cdscsdc */ }

object TypedDataFrameOpaqueScope:
  opaque type TypedDataFrame[+S <: FrameSchema] = UntypedDataFrame
  extension (inline df: UntypedDataFrame)
    inline def typed[A](using schema: FrameSchema.Provider[A]): TypedDataFrame[schema.Schema] = df // TODO: Check schema at runtime? Check if names of columns match?
    inline def withSchema[S <: FrameSchema]: TypedDataFrame[S] = df // TODO? make it private[typedframes]

  extension [S <: FrameSchema](tdf: TypedDataFrame[S])
    inline def untyped: UntypedDataFrame = tdf

export TypedDataFrameOpaqueScope.*