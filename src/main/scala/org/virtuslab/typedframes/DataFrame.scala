package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, Encoder, SparkSession }
import Internals.Name

object TypedDataFrameOpaqueScope:
  opaque type TypedDataFrame[+S <: TableSchema] = UntypedDataFrame
  extension (inline df: UntypedDataFrame)
    inline def typed[A](using schema: SchemaFor[A]): TypedDataFrame[schema.Schema] = df
    inline def withSchema[S <: TableSchema]: TypedDataFrame[S] = df // TODO? make it private[typedframes]
  extension [A](inline seq: Seq[A])(using schema: SchemaFor[A])(using encoder: Encoder[A], spark: SparkSession)
    inline def toTypedDF: TypedDataFrame[schema.Schema] =
      import spark.implicits.*
      seq.toDF(/* Should we explicitly pass columns here? */)

  extension [A <: Int | String](inline seq: Seq[A])(using spark: SparkSession) // TODO: Add more primitive types
    // TODO decide on/unify naming
    transparent inline def toTypedDF[N <: Name]: TypedDataFrame[TableSchema] = ${toTypedDFWithNameImpl[N, A]('seq, 'spark)}
    transparent inline def toTypedDFNamed[N <: Name](columnName: N): TypedDataFrame[TableSchema] = seq.toTypedDF[N]

  private def toTypedDFWithNameImpl[N <: Name : Type, A : Type](using Quotes)(seq: Expr[Seq[A]], spark: Expr[SparkSession]): Expr[TypedDataFrame[TableSchema]] =
    '{
      val s = $spark
      given Encoder[A] = ${ Expr.summon[Encoder[A]].get }
      import s.implicits.*
      localSeqToDatasetHolder($seq).toDF(valueOf[N])
    }

  extension [S <: TableSchema](tdf: TypedDataFrame[S])
    inline def untyped: UntypedDataFrame = tdf

export TypedDataFrameOpaqueScope.*