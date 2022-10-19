package org.virtuslab.iskra

import scala.language.implicitConversions

import org.virtuslab.iskra.types.Encoder

private[iskra] trait ApiCommons {
  implicit def toSeqTypingOps[A](seq: Seq[A])(implicit encoder: Encoder[A]): SeqTypingOps[A, encoder.type] = new SeqTypingOps(seq, encoder)
}