//> using target.scala "3"

package org.virtuslab.iskra

import scala.quoted.*
import org.apache.spark.sql
import org.virtuslab.iskra.types.{Encoder, StructEncoder}

private[iskra] object ToTypedDF:
  def toTypedDFImpl[A : Type](seq: Expr[Seq[A]], encoder: Expr[Encoder[A]], spark: Expr[SparkSession])(using Quotes) =
    val (schemaType, schema, encodeFun) = encoder match
      case '{ $e: StructEncoder.Aux[A, t] } =>
        val schema = '{ ${ e }.catalystType }
        val encodeFun: Expr[A => sql.Row] = '{ ${ e }.encode }
        (Type.of[t], schema, encodeFun)
      case '{ $e: Encoder.Aux[tpe, t] } =>
        val schema = '{
          sql.types.StructType(Seq(
            sql.types.StructField("value", ${ encoder }.catalystType, ${ encoder }.isNullable )
          ))
        }
        val encodeFun: Expr[A => sql.Row] = '{ (value: A) => sql.Row(${ encoder }.encode(value)) }
        (Type.of["value" := t], schema, encodeFun)

    schemaType match
      case '[StructSchema.Subtype[t]] => '{
        val rowRDD = ${ spark }.sparkContext.parallelize(${ seq }.map(${ encodeFun }))
        DataFrame[t](${ spark }.createDataFrame(rowRDD, ${ schema }))
      }