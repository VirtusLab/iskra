package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.functions.col
import types.{DataType, StructType}
import org.virtuslab.typedframes.types.StructType.{SNil, SCons}

trait SelectionView extends Selectable:
  type AllColumns <: Tuple
  def * : AllColumns
  def selectDynamic(name: String): TypedColumn[DataType] = TypedColumn[DataType](col(NamedColumn.escapeColumnName(name))) // TODO: Use columns bound to the data frame?

object SelectionView:

  trait Provider[A <: StructType]:
    type View <: SelectionView
    val view: View

  object Provider:
    transparent inline given selectionViewFor[A <: StructType]: Provider[A] = ${ selectionViewForImpl[A] }

    def selectionViewForImpl[A <: StructType : Type](using Quotes): Expr[SelectionView.Provider[A]] =
      import quotes.reflect.*

      selectionViewType(TypeRepr.of[SelectionView { type AllColumns = SchemaColumnsTuple[A] }], Type.of[A]).asType match
        case '[t] =>
          '{
            val v = new SelectionView {
              type AllColumns = SchemaColumnsTuple[A]
              override def * : AllColumns = ${ allColumns[A] }.asInstanceOf[AllColumns]
            }
            new SelectionView.Provider[A] {
              type View = t
              val view = v.asInstanceOf[t]
            }: SelectionView.Provider[A] { type View = t }
          }

    private def allColumns[A <: StructType : Type](using Quotes): Expr[Tuple] =
      allCols(Type.of[A])

    private def allCols(using Quotes)(schemaType: Type[?]): Expr[Tuple] =
      import quotes.reflect.*

      schemaType match
        case '[SNil] => '{ EmptyTuple }
        case '[SCons[headLabel, headType, tail]] =>
          val label = Expr(Type.valueOfConstant[headLabel].get.toString)
          '{ TypedColumn[Nothing](col(NamedColumn.escapeColumnName(${ label }))) *: ${ allCols(Type.of[tail]) } }

    private def selectionViewType(using Quotes)(base: quotes.reflect.TypeRepr, schemaType: Type[?]): quotes.reflect.TypeRepr =
      import quotes.reflect.*

      schemaType match
        case '[SNil] => base
        case '[SCons[headLabel, headType, tail]] => 
          val info = TypeRepr.of[NamedColumn[headLabel, headType]]
          val label = Type.valueOfConstant[headLabel].get.toString
          val newBase = Refinement(base, label, info)
          selectionViewType(newBase, Type.of[tail])

    type SchemaColumnsTuple[A <: StructType] <: Tuple = A match
      case StructType.SNil =>
        EmptyTuple
      case StructType.SCons[headLabel, headType, tail] =>
        NamedColumn[headLabel, headType] *: SchemaColumnsTuple[tail]
  end Provider