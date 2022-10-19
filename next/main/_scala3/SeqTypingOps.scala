//> using target.scala "3"

package org.virtuslab.iskra

import org.virtuslab.iskra.types.Encoder

class SeqTypingOps[A, E <: Encoder[A]](seq: Seq[A], encoder: E) {
  transparent inline def toTypedDF(using spark: SparkSession): DataFrame[?] = ${ ToTypedDF.toTypedDFImpl('seq, 'encoder, 'spark) }
} 