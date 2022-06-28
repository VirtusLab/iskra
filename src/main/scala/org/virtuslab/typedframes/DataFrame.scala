package org.virtuslab.typedframes

import org.apache.spark.sql
import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, SparkSession }
import scala.quoted.*
import types.{DataType, StructType}

class DataFrame[+S <: FrameSchema](val untyped: UntypedDataFrame) /* extends AnyVal */:
  type Alias <: Name

object DataFrame:
  export SelectOps.select
  export Join.joinOps

  type Subtype[T <: DataFrame[FrameSchema]] = T
  type WithAlias[T <: String & Singleton] = DataFrame[?] { type Alias = T }

  extension [S <: FrameSchema](tdf: DataFrame[S])
    inline def as[N <: Name](inline name: N)(using v: ValueOf[N]) =
      new DataFrame[FrameSchema.Reowned[S, N]](tdf.untyped.alias(v.value)):
        type Alias = N

  given untypedDataFrameOps: {} with
    extension (df: UntypedDataFrame)
      inline def typed[A](using encoder: FrameSchema.Encoder[A]): DataFrame[encoder.Encoded] = DataFrame(df) // TODO: Check schema at runtime? Check if names of columns match?

  given dataFrameOps: {} with
    extension [S <: FrameSchema](tdf: DataFrame[S])
      inline def show(): Unit = tdf.untyped.show()

      // TODO: check schema conformance instead of equality
      inline def collect[A]()(using typeEncoder: FrameSchema.Encoder[A], runtimeEncoder: sql.Encoder[A], eq: typeEncoder.Encoded =:= S): List[A] =
        tdf.untyped.as[A].collect.toList