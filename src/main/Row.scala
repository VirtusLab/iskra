package org.virtuslab.iskra

import scala.quoted.*


class Row[Schema](val columns: Seq[NamedColumn[?]])

object Row:
  transparent inline def apply(inline columns: NamedColumn[?]*): Row[?] = ${ applyImpl('columns) }

  private def applyImpl(columns: Expr[Seq[NamedColumn[?]]])(using Quotes): Expr[Row[?]] =
    import quotes.reflect.*
    val colTypes = columns match
      case Varargs(colExprs) =>
        colExprs.map{ case '{$col: c} => TypeRepr.of[c].widen.asType }
    val cols = columns match
      case Varargs(colExprs) => Expr.ofSeq(colExprs)
   
    val rowSchemaType = FrameSchema.schemaTypeFromColumnTypes(colTypes)

    rowSchemaType match
      case '[t] =>
        '{ new Row[t](${ cols }) }
        

