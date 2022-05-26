package org.virtuslab.typedframes

import org.apache.spark.sql.Encoder

extension [S <: FrameSchema](inline tdf: TypedDataFrame[S])
  inline def show(): Unit = tdf.untyped.show()

  // TODO: check schema conformance instead of equality
  inline def collect[T]()(using e: Encoder[T], fsf: FrameSchema.Provider[T], ev: fsf.Schema =:= S): List[T] =
    tdf.untyped.as[T].collect.toList