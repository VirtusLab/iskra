package org.virtuslab.typedframes

import scala.quoted.*
import MacroHelpers.TupleSubtype

trait Select[View <: SchemaView]:
  val view: View
  def underlying: UntypedDataFrame

object Select:
  given dataFrameSelectOps: {} with
    extension [DF <: DataFrame[?]](tdf: DF)
      transparent inline def select: Select[?] = ${ Select.selectImpl[DF]('{tdf}) }

  given selectApply: {} with
    extension [View <: SchemaView](select: Select[View])
      transparent inline def apply[Columns](columns: View ?=> Columns): DataFrame[?] =
        ${ Select.applyImpl[View, Columns]('select, 'columns) }

  def applyImpl[View <: SchemaView : Type, Columns : Type](
    select: Expr[Select[View]],
    columns: Expr[View ?=> Columns]
  )(using Quotes): Expr[DataFrame[?]] =
    import quotes.reflect.*
    Type.of[Columns] match
      case '[name := colType] =>
        '{
          DataFrame[name := colType](
            ${ select }.underlying.select(${ columns }(using ${ select }.view).asInstanceOf[name := colType].untyped)
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

  def selectImpl[DF <: DataFrame[?] : Type](tdf: Expr[DF])(using Quotes): Expr[Select[?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[DF]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new Select[v] {
            val view = ${ viewExpr }.asInstanceOf[v]
            def underlying = ${ tdf }.untyped
          }
        }
