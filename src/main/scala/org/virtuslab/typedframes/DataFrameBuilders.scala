package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.virtuslab.typedframes.DataFrame
import org.virtuslab.typedframes.types.{DataType, StructType}
import DataType.{Encoder, StructEncoder, PrimitiveEncoder}

object DataFrameBuilders:
  extension [A](seq: Seq[A])(using encoder: Encoder[A])
    transparent inline def toTypedDF(using spark: SparkSession): DataFrame[?] = ${ toTypedDFImpl('seq, 'encoder, 'spark) }

  private def toTypedDFImpl[A : Type](seq: Expr[Seq[A]], encoder: Expr[Encoder[A]], spark: Expr[SparkSession])(using Quotes) =
    val (schemaType, schema, encodeFun) = encoder match
      case '{ $e: DataType.StructEncoder.Aux[A, t] } =>
        val schema = '{ ${ e }.catalystType }
        val encodeFun: Expr[A => sql.Row] = '{ ${ e }.encode }
        (Type.of[t], schema, encodeFun)
      case '{ $e: DataType.Encoder.Aux[tpe, t] } =>
        val schema = '{
          sql.types.StructType(Seq(
            sql.types.StructField("value", ${ encoder }.catalystType, ${ encoder }.isNullable )
          ))
        }
        val encodeFun: Expr[A => sql.Row] = '{ (value: A) => sql.Row(${ encoder }.encode(value)) }
        (Type.of["value" := t], schema, encodeFun)

    schemaType match
      case '[t] => '{
        val rowRDD = ${ spark }.sparkContext.parallelize(${ seq }.map(${ encodeFun }))
        DataFrame[t](${ spark }.createDataFrame(rowRDD, ${ schema }))
      }
