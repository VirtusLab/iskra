package org.virtuslab.iskra

import scala.quoted.*
import MacroHelpers.TupleSubtype

class Select[View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object Select:
  given dataFrameSelectOps: {} with
    extension [DF <: StructDataFrame[?]](df: DF)
      transparent inline def select: Select[?] = ${ selectImpl[DF]('{df}) }

  def selectImpl[DF <: StructDataFrame[?] : Type](df: Expr[DF])(using Quotes): Expr[Select[?]] =
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
      transparent inline def apply[Columns](columns: View ?=> Columns): StructDataFrame[?] =
        ${ applyImpl[View, Columns]('select, 'columns) }

  def applyImpl[View <: SchemaView : Type, Columns : Type](
    select: Expr[Select[View]],
    columns: Expr[View ?=> Columns]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*
    Type.of[Columns] match
      case '[name := colType] =>
        '{
          StructDataFrame[name := colType](
            ${ select }.underlying.select(${ columns }(using ${ select }.view).asInstanceOf[Column[?]].untyped)
          )
        }
        
      case '[TupleSubtype[s]] if FrameSchema.isValidType(Type.of[s]) =>
        '{
          val cols = ${ columns }(using ${ select }.view).asInstanceOf[s].toList.map(_.asInstanceOf[Column[?]].untyped)
          StructDataFrame[s](${ select }.underlying.select(cols*))
        }

      case '[t] =>
        val errorMsg = s"""The parameter of `select` should be a named column (e.g. of type: "foo" := StringType) or a tuple of named columns but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(select))
