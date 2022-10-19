//> using target.scala "3"

package org.virtuslab.iskra
package contextual

import scala.quoted.*
import org.virtuslab.iskra.frame.SchemaView
import org.virtuslab.iskra.types.DataType

private[contextual] object SelectImpl:
  def selectImpl[Schema <: StructSchema : Type](df: Expr[DataFrame[Schema]])(using Quotes): Expr[Select[?]] =
    import quotes.reflect.asTerm
    val viewExpr = SchemaView.schemaViewExpr[Schema]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new Select[v](
            view = ${ viewExpr }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  def applyImpl[View <: SchemaView : Type, Columns : Type](
    select: Expr[Select[View]],
    columns: Expr[View ?=> Columns]
  )(using Quotes): Expr[DataFrame[?]] =
    import quotes.reflect.*
    Type.of[Columns] match
      case '[DataCol[DataType.Subtype[colType]] As name] =>
        '{
          DataFrame[name := colType](
            ${ select }.underlying.select(${ columns }(using ${ select }.view).asInstanceOf[Column].untyped)
          )
        }
        
      // case '[TupleSubtype[s]] if FrameSchema.isValidType(Type.of[s]) =>
      //   '{
      //     val cols = ${ columns }(using ${ select }.view).asInstanceOf[s].toList.map(_.asInstanceOf[Column].untyped)
      //     DataFrame[s](${ select }.underlying.select(cols*))
      //   }

      // TODO
      case '[t] =>
        val errorMsg = s"""The parameter of `select` should be a named data column (e.g. of type: DataCol[StringType] As "foo") or a tuple of named data columns but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(select))