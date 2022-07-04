package org.virtuslab.typedframes.types

import scala.quoted._
import scala.deriving.Mirror
import org.virtuslab.typedframes.Name

sealed trait DataType

object DataType:
  type Subtype[T <: DataType] = T
  
  trait Encoder[A]:
    type Encoded <: DataType

  object Encoder:
    type Aux[A, E <: DataType] = Encoder[A] { type Encoded = E }

    inline given boolean: Encoder[Boolean] with
      type Encoded = BooleanType
    inline given string: Encoder[String] with
      type Encoded = StringType
    inline given int: Encoder[Int] with
      type Encoded = IntegerType
    inline given float: Encoder[Float] with
      type Encoded = FloatType
    inline given double: Encoder[Double] with
      type Encoded = DoubleType

    export StructEncoder.fromMirror

  trait StructEncoder[A] extends Encoder[A]:
    type Encoded <: StructType

  object StructEncoder:
    def getEncodedType(using quotes: Quotes)(mirroredElemLabels: Type[?], mirroredElemTypes: Type[?]): quotes.reflect.TypeRepr =
      import quotes.reflect.*

      mirroredElemLabels match
        case '[EmptyTuple] => TypeRepr.of[StructType.SNil]
        case '[Name.Subtype[label] *: labels] => mirroredElemTypes match
          case '[tpe *: tpes] =>
            Expr.summon[Encoder[tpe]].getOrElse(fromMirrorImpl[tpe]) match
              case '{ ${encoder}: Encoder.Aux[tpe, DataType.Subtype[e]] } => 
                getEncodedType(Type.of[labels], Type.of[tpes]).asType match
                  case '[StructType.Subtype[tail]] =>
                    TypeRepr.of[StructType.SCons[label, e, tail]]

    transparent inline given fromMirror[A](using m: Mirror.ProductOf[A]): StructEncoder[A] = ${ fromMirrorImpl[A] }

    def fromMirrorImpl[A : Type](using Quotes): Expr[StructEncoder[A]] =
      val encodedType = Expr.summon[Mirror.Of[A]].getOrElse(throw new Exception(s"Could not find Mirror when generating encoder for ${Type.show[A]}")) match
        case '{ ${m}: Mirror.ProductOf[A] { type MirroredElemLabels = elementLabels; type MirroredElemTypes = elementTypes } } =>
          getEncodedType(Type.of[elementLabels], Type.of[elementTypes])
      encodedType.asType match
        case '[t] =>
          '{
            (new StructEncoder[A] {
              override type Encoded = t
            }): StructEncoder[A] { type Encoded = t }
          }


trait BooleanType extends DataType
trait StringType extends DataType
trait IntegerType extends DataType
trait FloatType extends DataType
trait DoubleType extends DataType


sealed trait StructType extends DataType

object StructType:
  object SNil extends StructType
  type SNil = SNil.type
  final case class SCons[N <: Name, H <: DataType, T <: StructType](headLabel: N, headTypeName: String, tail: T) extends StructType//:

  type Subtype[T <: StructType] = T

  type Merge[S1 <: StructType, S2 <: StructType] =
    S1 match
      case SNil => S2
      case SCons[headName, headType, tail] => SCons[headName, headType, Merge[tail, S2]]
