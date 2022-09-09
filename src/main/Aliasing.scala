package org.virtuslab.iskra

import scala.quoted.*

object Aliasing:
  def autoAliasImpl[DF <: DataFrame[?] : Type](df: Expr[DF])(using Quotes): Expr[DataFrame[?]] =
    import quotes.reflect.*

    val identName = df.asTerm match
      case Inlined(_, _, Ident(name)) =>
        Some(name)
      case Inlined(_, _, Select(This(_), name)) =>
        Some(name)
      case _ => None

    identName match
      case None => df
      case Some(name) =>
        Type.of[DF] match
          case '[DataFrame.WithAlias[alias]] => 
            df
          case '[DataFrame[schema]] =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] => FrameSchema.reownType[n](Type.of[schema]) match
                case '[t] =>
                  '{ DataFrame[t](${df}.untyped.as(${Expr(name)})) }
