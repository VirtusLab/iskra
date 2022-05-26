package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.functions.col

class SelectionView extends Selectable:
  def selectDynamic(name: String): TypedColumn[Nothing, Any] =
    UnnamedTypedColumn[Any](col(name))

object SelectionView:
  trait Provider[A <: FrameSchema]:
    type View <: SelectionView
    def view: View

  object Provider:
    transparent inline given selectionViewFor[A <: FrameSchema]: Provider[A] = ${selectionViewForImpl[A]}

    def selectionViewForImpl[A <: FrameSchema : Type](using Quotes): Expr[SelectionView.Provider[A]] =
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
      import FrameSchema.*
      import quotes.reflect.*

      frameType match
        case '[SNil] => base
        case '[SCons[headLabel, headType, tail]] => 
          val info = TypeRepr.of[TypedColumn[headLabel, headType]]
          val label = Type.valueOfConstant[headLabel].get.toString
          val newBase = Refinement(base, label, info)
          selectionView(newBase, Type.of[tail])
  end Provider