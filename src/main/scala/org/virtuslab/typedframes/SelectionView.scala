package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame => UntypedDataFrame}
import types.{DataType, StructType}
import org.virtuslab.typedframes.types.StructType.{SNil, SCons}

trait SelectionView extends Selectable:
  type AllColumns <: Tuple
  def * : AllColumns
  def selectDynamic(name: String): TypedColumn[DataType] = TypedColumn[DataType](col(NamedColumn.escapeColumnName(name)))

object SelectionView:
  type SchemaColumnsTuple[A <: StructType] <: Tuple = A match
    case StructType.SNil =>
      EmptyTuple
    case StructType.SCons[headLabel, headType, tail] =>
      NamedColumn[headLabel, headType] *: SchemaColumnsTuple[tail]

  private def allColumns[A <: StructType : Type](using Quotes): Expr[Tuple] = allCols(Type.of[A])

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

  def selectionViewExpr[DF <: TypedDataFrame[?] : Type](using Quotes): Expr[SelectionView] =
    import quotes.reflect.*
    Type.of[DF] match
      case '[TypedDataFrame[s]] =>
        // TODO: Hide ambiguous columns
        val viewType = selectionViewType(TypeRepr.of[SelectionView { type AllColumns = SchemaColumnsTuple[s] }], Type.of[s]).asType
        viewType match
          case '[t] => '{
            new SelectionView {
              type AllColumns = SchemaColumnsTuple[s]
              override def * : AllColumns =
                ${ allColumns[s] }.asInstanceOf[AllColumns]
            }.asInstanceOf[t & SelectionView] // TODO: Avoid cast?
          }
