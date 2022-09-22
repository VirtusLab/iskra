package org.virtuslab.iskra

import scala.quoted.*
import MacroHelpers.TupleSubtype

class Select[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object Select:
  given dataFrameSelectOps: {} with
    extension [DF <: DataFrame[?]](df: DF)
      transparent inline def select: Select[?] = ${ selectImpl[DF]('{df}) }

  def selectImpl[DF <: DataFrame[?] : Type](df: Expr[DF])(using Quotes): Expr[Select[?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[DF]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new Select[v](
            view = ${ viewExpr }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  given selectApply: {} with
    extension [View <: SchemaView](select: Select[View])
      transparent inline def apply[Columns](columns: View ?=> Columns): DataFrame[?] =
        ${ applyImpl[View, Columns]('select, 'columns) }

  def applyImpl[View <: SchemaView : Type, Columns : Type](
    select: Expr[Select[View]],
    columns: Expr[View ?=> Columns]
  )(using Quotes): Expr[DataFrame[?]] =
    import quotes.reflect.*
    Type.of[Columns] match
      case '[name := colType] =>
        '{
          DataFrame[name := colType](
            ${ select }.underlying.select(${ columns }(using ${ select }.view).asInstanceOf[Column[?]].untyped)
          )
        }
        
      case '[TupleSubtype[s]] if FrameSchema.isValidType(Type.of[s]) =>
        '{
          val cols = ${ columns }(using ${ select }.view).asInstanceOf[s].toList.map(_.asInstanceOf[Column[?]].untyped)
          DataFrame[s](${ select }.underlying.select(cols*))
        }

      case '[t] =>
        val errorMsg = s"""The parameter of `select` should be a named column (e.g. of type: "foo" := StringType) or a tuple of named columns but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(select))
