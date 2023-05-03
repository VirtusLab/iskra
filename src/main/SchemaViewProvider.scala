package org.virtuslab.iskra

import scala.quoted.*
import types.StructEncoder

trait SchemaViewProvider[A]:
  type View <: SchemaView
  def view: View

object SchemaViewProvider:
  transparent inline given derived[A]: SchemaViewProvider[A] = ${ derivedImpl[A]}

  private def derivedImpl[A : Type](using Quotes): Expr[SchemaViewProvider[A]] =
    import quotes.reflect.*

      Expr.summon[StructEncoder[A]] match
        case Some(encoder) => encoder match
          case '{ $enc: StructEncoder[A] { type StructSchema = structSchema } } =>
            val schemaView = StructSchemaView.schemaViewExpr[StructDataFrame[structSchema]]
            schemaView.asTerm.tpe.asType match
              case '[SchemaView.Subtype[v]] =>
                '{ 
                  new SchemaViewProvider[A] {
                    override type View = v
                    override def view = ${ schemaView }.asInstanceOf[v]
                  }
                }
        case None =>
          report.errorAndAbort(s"SchemaViewProvider cannot be derived for type ${Type.show[A]} because a given instance of Encoder is missing")
