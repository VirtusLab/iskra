package org.virtuslab.iskra

import scala.compiletime.{constValue, error}

trait Error

object Error:
  type Subtype[E <: Error] = E

  trait Show[E]:
    inline def shown: String

trait Err[-Msg <: String & Singleton] extends Error
object Err:
  given show[Msg <: String & Singleton](using v: ValueOf[Msg]): Error.Show[Err[Msg]] with
    inline def shown = constValue[Msg]
