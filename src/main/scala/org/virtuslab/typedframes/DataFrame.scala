package org.virtuslab.typedframes

import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, Encoder, SparkSession }
import scala.quoted.*
import types.{DataType, StructType}
import Internals.Name

class TypedDataFrame[+S <: FrameSchema](val untyped: UntypedDataFrame) /* extends AnyVal */:
  type Alias <: Name

object TypedDataFrame:
  type Subtype[T <: TypedDataFrame[FrameSchema]] = T
  type WithAlias[T <: String & Singleton] = TypedDataFrame[?] { type Alias = T }

  extension [S <: FrameSchema](tdf: TypedDataFrame[S])
    inline def as[N <: Name](inline name: N)(using v: ValueOf[N]) =
      new TypedDataFrame[FrameSchema.Reowned[S, N]](tdf.untyped.alias(v.value)):
        type Alias = N
  
  def autoAliasTypeImpl[DF <: TypedDataFrame[FrameSchema] : Type](df: Expr[DF])(using Quotes): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    df.asTerm match
      case Inlined(_, _, Ident(name)) =>
        Type.of[DF] match
          case '[TypedDataFrame.WithAlias[alias]] => 
            TypeRepr.of[DF]
          case '[TypedDataFrame[schema]] =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] => TypeRepr.of[TypedDataFrame[FrameSchema.Reowned[schema, n]] { type Alias = n }]
      case _ => TypeRepr.of[DF]

  def autoAliasImpl[DF <: TypedDataFrame[FrameSchema] : Type](df: Expr[DF])(using Quotes): Expr[UntypedDataFrame] =
    import quotes.reflect.*
    df.asTerm match
      case Inlined(_, _, Ident(name)) =>
        Type.of[DF] match
          case '[TypedDataFrame.WithAlias[alias]] =>
            '{ ${df}.untyped }
          case _ =>
            val nameExpr = Expr(name)
            '{ ${df}.untyped.as(${nameExpr}) }
      case _ => '{ ${df}.untyped }

given untypedDataFrameOps: {} with
  extension (df: UntypedDataFrame)
    inline def typed[A](using encoder: FrameSchema.Encoder[A]): TypedDataFrame[encoder.Encoded] = TypedDataFrame(df) // TODO: Check schema at runtime? Check if names of columns match?
    inline def withSchema[S <: FrameSchema]: TypedDataFrame[S] = TypedDataFrame(df) // TODO? make it private[typedframes]
