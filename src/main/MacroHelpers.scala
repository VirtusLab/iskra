package org.virtuslab.iskra

import scala.quoted.*

private[iskra] object MacroHelpers:
  def callPosition(ownerExpr: Expr[?])(using Quotes): quotes.reflect.Position =
    import quotes.reflect.*
    val file = Position.ofMacroExpansion.sourceFile
    val start = ownerExpr.asTerm.pos.end
    val end = Position.ofMacroExpansion.end
    Position(file, start, end)

  type TupleSubtype[T <: Tuple] = T

  type AsTuple[A] <: Tuple = A match
    case TupleSubtype[t] => t
    case _ => A *: EmptyTuple