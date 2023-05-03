package org.virtuslab.iskra

import scala.quoted._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.virtuslab.iskra.DataFrame
import org.virtuslab.iskra.types.{DataType, StructType, Encoder, StructEncoder, PrimitiveEncoder}

object DataFrameBuilders:
  extension [A](seq: Seq[A])(using encoder: Encoder[A])
    inline def toDF(using spark: SparkSession): ClassDataFrame[A] = ${ toTypedDFImpl('seq, 'encoder, 'spark) }

  private def toTypedDFImpl[A : Type](seq: Expr[Seq[A]], encoder: Expr[Encoder[A]], spark: Expr[SparkSession])(using Quotes) =    
    val (schema, encodeFun) = encoder match
      case '{ $e: StructEncoder.Aux[A, t] } =>
        val schema = '{ ${ e }.catalystType }
        val encodeFun: Expr[A => sql.Row] = '{ ${ e }.encode }
        (schema, encodeFun)
      case '{ $e: Encoder.Aux[tpe, t] } =>
        val schema = '{
          sql.types.StructType(Seq(
            sql.types.StructField("value", ${ encoder }.catalystType, ${ encoder }.isNullable )
          ))
        }
        val encodeFun: Expr[A => sql.Row] = '{ (value: A) => sql.Row(${ encoder }.encode(value)) }
        (schema, encodeFun)

    '{
      val rowRDD = ${ spark }.sparkContext.parallelize(${ seq }.map(${ encodeFun }))
      ClassDataFrame[A](${ spark }.createDataFrame(rowRDD, ${ schema }))
    }
