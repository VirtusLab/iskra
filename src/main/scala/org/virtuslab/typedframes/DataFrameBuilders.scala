package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql
import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, SparkSession }
import org.virtuslab.typedframes.DataFrame
import org.virtuslab.typedframes.DataFrame.untypedDataFrameOps
import org.virtuslab.typedframes.types.DataType

object DataFrameBuilders:
  given primitiveTypeBuilderOps: {} with
    extension [A <: Int | String | Boolean](seq: Seq[A])(using typeEncoder: DataType.Encoder[A], spark: SparkSession) // TODO: Add more primitive types
      transparent inline def toTypedDF[N <: Name](name: N): DataFrame[?] = ${ toTypedDFWithNameImpl[N, A, typeEncoder.Encoded]('seq, 'spark) }

  private def toTypedDFWithNameImpl[N <: Name : Type, A : Type, E <: DataType : Type](using Quotes)(seq: Expr[Seq[A]], spark: Expr[SparkSession]): Expr[DataFrame[?]] =
    '{
      val s = ${spark}
      given sql.Encoder[A] = ${ Expr.summon[sql.Encoder[A]].get }
      import s.implicits.*
      DataFrame[Tuple1[LabeledColumn[N, E]]](
        localSeqToDatasetHolder(${seq}).toDF(valueOf[N])
      )
    }

  given complexTypeBuilderOps: {} with
    extension [A](seq: Seq[A])(using typeEncoder: FrameSchema.Encoder[A], runtimeEncoder: sql.Encoder[A], spark: SparkSession)
      inline def toTypedDF: DataFrame[typeEncoder.Encoded] =
        import spark.implicits.*
        seq.toDF(/* Should we explicitly pass columns here? */).typed
