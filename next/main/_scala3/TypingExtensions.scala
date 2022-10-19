// //> using target.scala "3"

// package org.virtuslab.iskra

// import scala.quoted.*
// import org.apache.spark.sql
// import org.apache.spark.sql.SparkSession
// import org.virtuslab.iskra.DataFrame
// import org.virtuslab.iskra.types.{StructType, Encoder, StructEncoder}

// object TypingExtensions:
//   // private def toTypedDFImpl[A : Type](seq: Expr[Seq[A]], encoder: Expr[Encoder[A]], spark: Expr[SparkSession])(using Quotes) =
//   //   val (schemaType, schema, encodeFun) = encoder match
//   //     case '{ $e: StructEncoder.Aux[A, t] } =>
//   //       val schema = '{ ${ e }.catalystType }
//   //       val encodeFun: Expr[A => sql.Row] = '{ ${ e }.encode }
//   //       (Type.of[t], schema, encodeFun)
//   //     case '{ $e: Encoder.Aux[tpe, t] } =>
//   //       val schema = '{
//   //         sql.types.StructType(Seq(
//   //           sql.types.StructField("value", ${ encoder }.catalystType, ${ encoder }.isNullable )
//   //         ))
//   //       }
//   //       val encodeFun: Expr[A => sql.Row] = '{ (value: A) => sql.Row(${ encoder }.encode(value)) }
//   //       (Type.of["value" := t], schema, encodeFun)

//   //   schemaType match
//   //     case '[StructSchema.Subtype[t]] => '{
//   //       val rowRDD = ${ spark }.sparkContext.parallelize(${ seq }.map(${ encodeFun }))
//   //       DataFrame[t](${ spark }.createDataFrame(rowRDD, ${ schema }))
//   //     }

//   class SeqTypingOps[A, E <: Encoder[A]](seq: Seq[A])(using encoder: E) {
//     transparent inline def toTypedDF(using spark: SparkSession): DataFrame[?] = ${ TypingExtensions.toTypedDFImpl('seq, 'encoder, 'spark) }
//   } 

// import TypingExtensions.*

// trait TypingExtensions {
//   import scala.language.implicitConversions
//   implicit def DataFrameTypingOps[A](seq: Seq[A])(implicit encoder: Encoder[A]): DataFrameTypingOps[A, encoder.type] = new DataFrameTypingOps(seq)
// }
