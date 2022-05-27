package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.functions.col
import types.{DataType, StructType}

class SelectionView extends Selectable:
  def selectDynamic(name: String): TypedColumn[Nothing, DataType] =
    UnnamedTypedColumn[DataType](col(name))

object SelectionView:
  trait Provider[A <: StructType]:
    type View <: SelectionView
    def view: View

  object Provider:
    transparent inline given selectionViewFor[A <: StructType]: Provider[A] = ${selectionViewForImpl[A]}

    def selectionViewForImpl[A <: StructType : Type](using Quotes): Expr[SelectionView.Provider[A]] =
      import quotes.reflect.*

      selectionView(TypeRepr.of[SelectionView], Type.of[A]).asType match
        case '[t] =>
          '{
            new SelectionView.Provider[A] {
              type View = t
              def view = (new SelectionView {}).asInstanceOf[t]
            }: SelectionView.Provider[A] { type View = t }
          }

    private def selectionView(using Quotes)(base: quotes.reflect.TypeRepr, frameType: Type[?]): quotes.reflect.TypeRepr =
      import quotes.reflect.*
      import StructType.{SNil, SCons}

      frameType match
        case '[SNil] => base
        case '[SCons[headLabel, headType, tail]] => 
          val info = TypeRepr.of[TypedColumn[headLabel, headType]]
          val label = Type.valueOfConstant[headLabel].get.toString
          val newBase = Refinement(base, label, info)
          selectionView(newBase, Type.of[tail])
  end Provider