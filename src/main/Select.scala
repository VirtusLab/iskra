package org.virtuslab.iskra

import scala.quoted.*

class Select[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object Select:
  given dataFrameSelectOps: {} with
    extension [DF <: DataFrame](df: DF)
      transparent inline def select: Select[?] = ${ selectImpl[DF]('{df}) }

  private def selectImpl[DF <: DataFrame : Type](df: Expr[DF])(using Quotes): Expr[Select[?]] =
    import quotes.reflect.{asTerm, report}

    val schemaView = SchemaView.exprForDataFrame[DF].getOrElse(
      report.errorAndAbort(s"A given instance of SchemaViewProvider for the model type of ${Type.show[DF]} is required to make `.select` possible")
    )

    schemaView.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new Select[v](
            view = ${ schemaView }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  given selectOps: {} with
    extension [View <: SchemaView](select: Select[View])
      transparent inline def apply[C <: NamedColumns](columns: View ?=> C): StructDataFrame[?] =
        ${ applyImpl[View, C]('select, 'columns) }

  private def applyImpl[View <: SchemaView : Type, C : Type](using Quotes)(select: Expr[Select[View]], columns: Expr[View ?=> C]) =
    import quotes.reflect.*

    Expr.summon[CollectColumns[C]] match
      case Some(collectColumns) =>
        collectColumns match
          case '{ $cc: CollectColumns[?] { type CollectedColumns = collectedColumns } } =>
            Type.of[collectedColumns] match
              case '[head *: EmptyTuple] =>
                '{
                  val cols = ${ cc }.underlyingColumns(${ columns }(using ${ select }.view))
                  StructDataFrame[head](${ select }.underlying.select(cols*))
                }

              case '[s] =>
                '{
                  val cols = ${ cc }.underlyingColumns(${ columns }(using ${ select }.view))
                  StructDataFrame[s](${ select }.underlying.select(cols*))
                }
      case None =>
        throw CollectColumns.CannotCollectColumns(Type.show[C])
