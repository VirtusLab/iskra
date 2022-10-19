// //> using target.scala "2"

// package org.virtuslab.iskra

// trait TypingExtensions {
//   // implicit class DataFrameTypingOps[A](seq: Seq[A])(implicit encoder: Encoder[A]) {
//   //   transparent inline def toTypedDF(using spark: SparkSession): DataFrame.WithSchema[?] = ${ toTypedDFImpl('seq, 'encoder, 'spark) }
//   // }
// }