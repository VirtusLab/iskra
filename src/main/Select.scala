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
      transparent inline def apply(inline columns: View ?=> NamedColumns[?]*): StructDataFrame[?] =
        ${ applyImpl[View]('select, 'columns) }

  private def applyImpl[View <: SchemaView : Type](using Quotes)(select: Expr[Select[View]], columns: Expr[Seq[View ?=> NamedColumns[?]]]) =
    import quotes.reflect.*

    val columnValuesWithTypes = columns match
      case Varargs(colExprs) =>
        colExprs.map { arg =>
          val reduced = Term.betaReduce('{$arg(using ${ select }.view)}.asTerm).get
          reduced.asExpr match
            case '{ $value: NamedColumns[schema] } => ('{ ${ value }.underlyingColumns }, Type.of[schema])
        }

    val columnsValues = columnValuesWithTypes.map(_._1)
    val columnsTypes = columnValuesWithTypes.map(_._2)

    val schemaTpe = FrameSchema.schemaTypeFromColumnsTypes(columnsTypes)
    schemaTpe match
      case '[s] =>
        '{
          val cols = ${ Expr.ofSeq(columnsValues) }.flatten
          StructDataFrame[s](${ select }.underlying.select(cols*))
        }
