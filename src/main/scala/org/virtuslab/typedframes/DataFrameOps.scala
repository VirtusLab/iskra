package org.virtuslab.typedframes

import org.apache.spark.sql
import types.{DataType, StructType}

extension [S <: FrameSchema](tdf: TypedDataFrame[S])
  inline def show(): Unit = tdf.untyped.show()

  // TODO: check schema conformance instead of equality
  inline def collect[A]()(using typeEncoder: FrameSchema.Encoder[A], runtimeEncoder: sql.Encoder[A], eq: typeEncoder.Encoded =:= S): List[A] =
    tdf.untyped.as[A].collect.toList