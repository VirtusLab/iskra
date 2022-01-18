package org.virtuslab.typedframes

import scala.quoted._
import scala.deriving.Mirror
import org.apache.spark.sql.functions.col
import Internals.Name

class TableSchema extends Selectable:
  def selectDynamic(name: String): TypedColumn[Nothing, Any] =
    UnnamedTypedColumn[Any](col(name))

trait SchemaFor[A]:
  type Schema <: TableSchema

transparent inline given schemaFromMirror[A : Mirror.ProductOf]: SchemaFor[A] = ${schemaFromMirrorImpl[A]}

def schemaFromMirrorImpl[A : Type](using Quotes): Expr[SchemaFor[A]] =
  productSchemaType[A].asType match
    case '[t] =>
      '{
        new SchemaFor[A] {
          type Schema = t
        }.asInstanceOf[SchemaFor[A] { type Schema = t }]
      }

transparent inline def schemaOfProduct[T]: TableSchema = ${ schemaOfProductImpl[T] }

private def productSchemaType[T : Type](using Quotes): quotes.reflect.TypeRepr =
  import quotes.reflect.*
  Expr.summon[Mirror.ProductOf[T]].get match
    case '{ $m: Mirror.ProductOf[T] {type MirroredElemLabels = mels; type MirroredElemTypes = mets } } =>
      schemaTypeWithColumns[mels, mets]

private def makeSchemaInstance(using Quotes)(tp: Type[?]): Expr[TableSchema] =
  tp match
    case '[tpe] =>
      val res = '{
        val p = TableSchema()
        p.asInstanceOf[TableSchema & tpe]
      }
      res

private def schemaOfProductImpl[T : Type](using Quotes): Expr[TableSchema] = 
  val tpe = productSchemaType[T]
  makeSchemaInstance(tpe.asType)

type NameLike[T <: Name] = T

private def schemaTypeWithColumns(using Quotes)(elementLabels: quotes.reflect.TypeRepr, elementTypes: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
  import quotes.reflect.*
  refineWithColumns(TypeRepr.of[TableSchema], elementLabels, elementTypes)

private def schemaTypeWithColumns[Labels : Type, Types : Type](using Quotes): quotes.reflect.TypeRepr =
  import quotes.reflect.*
  schemaTypeWithColumns(TypeRepr.of[Labels], TypeRepr.of[Types])

private def refineWithColumns(using Quotes)(base: quotes.reflect.TypeRepr, elementLabels: quotes.reflect.TypeRepr, elementTypes: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
  import quotes.reflect.*

  elementLabels.asType match
    case '[NameLike[elemLabel] *: elemLabels] =>
      val label = Type.valueOfConstant[elemLabel].get.toString
      elementTypes.asType match
        case '[elemType *: elemTypes] =>
          val info = TypeRepr.of[TypedColumn[elemLabel, elemType]]
          val newBase = Refinement(base, label, info)
          refineWithColumns(newBase, TypeRepr.of[elemLabels], TypeRepr.of[elemTypes])
    case '[EmptyTuple] => base