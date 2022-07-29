package org.virtuslab.typedframes

import scala.quoted.*

object Aliasing:
  def autoAliasTypeImpl[DF <: DataFrame[?] : Type](df: Expr[DF])(using Quotes): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    df.asTerm match
      case Inlined(_, _, Ident(name)) =>
        Type.of[DF] match
          case '[DataFrame.WithAlias[alias]] => 
            TypeRepr.of[DF]
          case '[DataFrame[schema]] =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] => FrameSchema.reownType[n](Type.of[schema]) match
                case '[t] => TypeRepr.of[DataFrame[t]]
      case _ => TypeRepr.of[DF]

  def autoAliasImpl[DF <: DataFrame[?] : Type](df: Expr[DF])(using Quotes): Expr[UntypedDataFrame] =
    import quotes.reflect.*
    df.asTerm match
      case Inlined(_, _, Ident(name)) =>
        Type.of[DF] match
          case '[DataFrame.WithAlias[alias]] =>
            '{ ${df}.untyped }
          case _ =>
            val nameExpr = Expr(name)
            '{ ${df}.untyped.as(${nameExpr}) }
      case _ => '{ ${df}.untyped }