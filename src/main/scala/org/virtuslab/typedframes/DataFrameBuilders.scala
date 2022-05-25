package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, Encoder, SparkSession /* cdscsdc */ }
import Internals.Name
import TypedDataFrameOpaqueScope.*

object TypedDataFrameBuilders:
  //TODO: More inlining? 
  class UnnamedColumnBuilder[A <: Int | String](spark: SparkSession, seq: Seq[A]):
    transparent inline def withColumn[N <: Name]: TypedDataFrame[FrameSchema] = ${toTypedDFWithNameImpl[N, A]('seq, 'spark)}
    transparent inline def withColumn[N <: Name](columnName: N): TypedDataFrame[FrameSchema] = withColumn[N]

  given foo1: {} with
    extension [A <: Int | String](inline seq: Seq[A])(using spark: SparkSession) // TODO: Add more primitive types
      // TODO decide on/unify naming
      // transparent inline def toTypedDF[N <: Name]: TypedDataFrame[FrameSchema] = ${toTypedDFWithNameImpl[N, A]('seq, 'spark)}
      inline def toTypedDF: UnnamedColumnBuilder[A] = new UnnamedColumnBuilder[A](spark, seq)
  
  // given foo2: {} with
  //   extension [A <: Int | String](inline seq: Seq[A])(using spark: SparkSession) // TODO: Add more primitive types
  //     // transparent inline def toTypedDF[N <: Name](columnName: N): TypedDataFrame[FrameSchema] = seq.toTypedDF[N]
  //     transparent inline def toTypedDFNamed[N <: Name](columnName: N): TypedDataFrame[FrameSchema] = seq.toTypedDF[N]

  private def toTypedDFWithNameImpl[N <: Name : Type, A : Type](using Quotes)(seq: Expr[Seq[A]], spark: Expr[SparkSession]): Expr[TypedDataFrame[FrameSchema/* TableSchema */]] =
    '{
      val s = $spark
      given Encoder[A] = ${ Expr.summon[Encoder[A]].get }
      import s.implicits.*
      localSeqToDatasetHolder($seq).toDF(valueOf[N]).withSchema[FrameSchema.WithSingleColumn[N, A]]
    }

  given foo3: {} with
    extension [A](inline seq: Seq[A])(using schema: FrameSchemaFor[A])(using encoder: Encoder[A], spark: SparkSession)
      inline def toTypedDF: TypedDataFrame[schema.Schema] =
        import spark.implicits.*
        seq.toDF(/* Should we explicitly pass columns here? */).typed

export TypedDataFrameBuilders.given