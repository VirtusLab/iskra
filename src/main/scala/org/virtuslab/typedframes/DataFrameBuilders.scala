package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql
import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, SparkSession }
import types.{ DataType, StructType }
import Internals.Name

object TypedDataFrameBuilders:
  given primitiveTypeBuilderOps: {} with
    extension [A <: Int | String](seq: Seq[A])(using typeEncoder: DataType.Encoder[A], spark: SparkSession) // TODO: Add more primitive types
      transparent inline def toTypedDF[N <: Name](name: N): TypedDataFrame[StructType] = ${toTypedDFWithNameImpl[N, A, typeEncoder.Encoded]('seq, 'spark)}

  private def toTypedDFWithNameImpl[N <: Name : Type, A : Type, E <: DataType : Type](using Quotes)(seq: Expr[Seq[A]], spark: Expr[SparkSession]): Expr[TypedDataFrame[StructType/* TableSchema */]] =
    '{
      val s = $spark
      given sql.Encoder[A] = ${ Expr.summon[sql.Encoder[A]].get }
      import s.implicits.*
      localSeqToDatasetHolder($seq).toDF(valueOf[N]).withSchema[StructType.WithSingleColumn[N, E]]
    }

  given structTypeBuilderOps: {} with
    extension [A](seq: Seq[A])(using typeEncoder: DataType.StructEncoder[A], runtimeEncoder: sql.Encoder[A], spark: SparkSession)
      inline def toTypedDF: TypedDataFrame[typeEncoder.Encoded] =
        import spark.implicits.*
        seq.toDF(/* Should we explicitly pass columns here? */).typed

export TypedDataFrameBuilders.given