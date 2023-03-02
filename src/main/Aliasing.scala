package org.virtuslab.iskra

import scala.quoted.*

object Aliasing:
  given dataFrameAliasingOps: {} with
    extension [S](df: StructDataFrame[S])
      transparent inline def as(inline frameName: Name): StructDataFrame[?] = ${ Aliasing.aliasStructDataFrameImpl('df, 'frameName) }
      transparent inline def alias(inline frameName: Name): StructDataFrame[?] = ${ Aliasing.aliasStructDataFrameImpl('df, 'frameName) }

  def autoAliasImpl[DF <: StructDataFrame[?] : Type](df: Expr[DF])(using Quotes): Expr[StructDataFrame[?]] =
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
          case '[StructDataFrame.WithAlias[alias]] => 
            df
          case '[StructDataFrame[schema]] =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] => FrameSchema.reownType[n](Type.of[schema]) match
                case '[t] =>
                  '{ StructDataFrame[t](${df}.untyped.as(${Expr(name)})) }


  def aliasStructDataFrameImpl[S : Type](df: Expr[StructDataFrame[S]], frameName: Expr[String])(using Quotes) =
    import quotes.reflect.*
    ConstantType(StringConstant(frameName.valueOrAbort)).asType match
      case '[Name.Subtype[n]] =>
        FrameSchema.reownType[n](Type.of[S]) match
          case '[reowned] => '{
            new StructDataFrame[reowned](${ df }.untyped.alias(${ frameName })):
              type Alias = n
          }
