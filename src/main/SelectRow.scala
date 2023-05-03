package org.virtuslab.iskra

import scala.quoted.*

class SelectRow[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object SelectRow:
  given dataFrameSelectRowOps: {} with
    extension [DF <: DataFrame](df: DF)
      transparent inline def selectRow: SelectRow[?] = ${ selectRowImpl[DF]('{df}) }

  private def selectRowImpl[DF <: DataFrame : Type](df: Expr[DF])(using Quotes): Expr[SelectRow[?]] =
    import quotes.reflect.{asTerm, report}

    val schemaView = SchemaView.exprForDataFrame[DF].getOrElse(
      report.errorAndAbort(s"A given instance of SchemaViewProvider for the model type of ${Type.show[DF]} is required to make `.selectRow` possible")
    )

    schemaView.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new SelectRow[v](
            view = ${ schemaView }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  given selectRowOps: {} with
    extension [View <: SchemaView](select: SelectRow[View])
      inline def apply[Schema](row: View ?=> Row[Schema]): StructDataFrame[Schema] =
        ${ selectRowApplyImpl[View, Schema]('select, '{row(using select.view)})}


  private def selectRowApplyImpl[View <: SchemaView : Type, Schema : Type](using Quotes)(select: Expr[SelectRow[View]], row: Expr[Row[Schema]]): Expr[StructDataFrame[Schema]] =
    '{
      val cols = ${ row }.columns.map(_.asInstanceOf[Column[?]].untyped)
      StructDataFrame[Schema](${ select }.underlying.select(cols*))
    }
