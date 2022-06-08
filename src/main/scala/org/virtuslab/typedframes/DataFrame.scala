package org.virtuslab.typedframes

import org.apache.spark.sql.{ DataFrame => UntypedDataFrame, Encoder, SparkSession }
import scala.quoted.*
import types.{DataType, StructType}
import Internals.Name

// object TypedDataFrameOpaqueScope:
//   opaque type TypedDataFrame[+S <: StructType] = UntypedDataFrame
//   extension (inline df: UntypedDataFrame)
//     inline def typed[A](using encoder: DataType.StructEncoder[A]): TypedDataFrame[encoder.Encoded] = df // TODO: Check schema at runtime? Check if names of columns match?
//     inline def withSchema[S <: StructType]: TypedDataFrame[S] = df // TODO? make it private[typedframes]

//   extension [S <: StructType](tdf: TypedDataFrame[S])
//     inline def untyped: UntypedDataFrame = tdf

// export TypedDataFrameOpaqueScope.*

class TypedDataFrame[+S <: StructType](val untyped: UntypedDataFrame) /* extends AnyVal */:
  type Alias <: Name

  inline def as[N <: Name](inline name: N) = this.asInstanceOf[TypedDataFrame[S] { type Alias = N }]

object TypedDataFrame:
  transparent inline def autoAlias[DF <: TypedDataFrame[?]](inline df: DF) = ${ TypedDataFrame.autoAliasImpl[DF]('{ df }) }

  def autoAliasImpl[DF <: TypedDataFrame[?] : Type](df: Expr[DF])(using Quotes): Expr[TypedDataFrame[?]] =
    import quotes.reflect.*
    df.asTerm match
      case Inlined(_, _, Ident(name)) =>
        ConstantType(StringConstant(name)).asType match
          case '[t] => '{ ${ df }.asInstanceOf[DF { type Alias = t }] }
      case _ => df

  type Aux[+S <: StructType, A <: Name] = TypedDataFrame[StructType] { type Alias = A }

given untypedDataFrameOps: {} with
  extension (df: UntypedDataFrame)
    inline def typed[A](using encoder: DataType.StructEncoder[A]): TypedDataFrame[encoder.Encoded] = TypedDataFrame(df) // TODO: Check schema at runtime? Check if names of columns match?
    inline def withSchema[S <: StructType]: TypedDataFrame[S] = TypedDataFrame(df) // TODO? make it private[typedframes]
