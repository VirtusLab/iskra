package org.virtuslab.typedframes

import scala.quoted.*

private[typedframes] object MacroHelpers:
  def callPosition(ownerExpr: Expr[?])(using Quotes): quotes.reflect.Position =
    import quotes.reflect.*
    val file = Position.ofMacroExpansion.sourceFile
    val start = ownerExpr.asTerm.pos.end
    val end = Position.ofMacroExpansion.end
    Position(file, start, end)