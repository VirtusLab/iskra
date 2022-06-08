package org.virtuslab.typedframes.types

import scala.quoted._
import scala.deriving.Mirror
import scala.compiletime.{erasedValue, summonInline}
import org.virtuslab.typedframes.Internals.Name

trait DataType

object DataType:
  type Subtype[T <: DataType] = T
  
  trait Encoder[A]:
    type Encoded <: DataType

  object Encoder:
    type Aux[A, E <: DataType] = Encoder[A] { type Encoded = E }

    inline given int: Encoder[Int] with
      type Encoded = IntegerType
    inline given string: Encoder[String] with
      type Encoded = StringType

    type EncodedTypes[T <: Tuple] = T match
      case EmptyTuple => EmptyTuple
      case Encoder.Aux[_, e] *: tail => e *: EncodedTypes[tail]

  trait StructEncoder[A] extends Encoder[A]:
    type Encoded <: StructType

  object StructEncoder:
    transparent inline def summonEncoders[Ts <: Tuple] = ${ summonEncodersImpl[Ts] }

    def summonEncodersImpl[Ts <: Tuple : Type](using Quotes): Expr[Tuple] =
      Expr.ofTupleFromSeq(summonAllEncoders[Ts])

    def summonAllEncoders[Ts <: Tuple : Type](using Quotes): List[Expr[Any]] =
      Type.of[Ts] match
        case '[ head *: tail ] =>
          Expr.summon[Encoder[head]] match
            case Some(headExpr) => headExpr :: summonAllEncoders[tail]
            case _ => quotes.reflect.report.throwError(s"Could not summon ${Type.show[Encoder[head]]}")
        case '[ EmptyTuple ] => Nil
        case _ => quotes.reflect.report.throwError(s"Could not `summonAllEncoders` for: ${Type.show[Ts]}")

    def getEncodedType(using quotes: Quotes)(mirroredElemLabels: Type[?], mirroredElemTypes: Type[?]): quotes.reflect.TypeRepr =
      import quotes.reflect.*

      mirroredElemLabels match
        case '[EmptyTuple] => TypeRepr.of[StructType.SNil]
        case '[Name.Subtype[label] *: labels] => mirroredElemTypes match
          case '[tpe *: tpes] =>
            Expr.summon[Encoder[tpe]].getOrElse(fromMirrorImpl[tpe]) match // TODO: tpe might also be an unhandled primitive
              case '{ $encoder: Encoder.Aux[tpe, DataType.Subtype[e]] } => 
                getEncodedType(Type.of[labels], Type.of[tpes]).asType match
                  case '[StructType.Subtype[tail]] =>
                    TypeRepr.of[StructType.SCons[label, e, tail]]

    transparent inline given fromMirror[A](using m: Mirror.ProductOf[A]): StructEncoder[A] = ${ fromMirrorImpl[A] }

    def fromMirrorImpl[A : Type](using Quotes): Expr[StructEncoder[A]] =
      val encodedType = Expr.summon[Mirror.Of[A]].getOrElse(throw new Exception(s"aaaaa ${Type.show[A]}")) match
        case '{ $m: Mirror.ProductOf[A] { type MirroredElemLabels = elementLabels; type MirroredElemTypes = elementTypes } } =>
          getEncodedType(Type.of[elementLabels], Type.of[elementTypes])
      encodedType.asType match
        case '[t] =>
          '{
            (new StructEncoder[A] {
              override type Encoded = t
            }): StructEncoder[A] { type Encoded = t }
          }

trait IntegerType extends DataType
trait StringType extends DataType
trait BooleanType extends DataType

sealed trait StructType extends DataType
object StructType:
  object SNil extends StructType
  type SNil = SNil.type
  final case class SCons[N <: Name, H <: DataType, T <: StructType](headLabel: N, headTypeName: String, tail: T) extends StructType//:

  type Subtype[T <: StructType] = T

  type FromLabelsAndTypes[Ls <: Tuple, Ts <: Tuple] <: StructType = Ls match
    case Name.Subtype[elemLabel] *: elemLabels => Ts match
      case elemType *: elemTypes =>
        elemType match
          case DataType.Subtype[elemTpe] =>
            StructType.SCons[elemLabel, elemTpe, FromLabelsAndTypes[elemLabels, elemTypes]]
    case EmptyTuple => StructType.SNil

  type WithSingleColumn[N <: Name, ColType <: DataType] = FromLabelsAndTypes[N *: EmptyTuple, ColType *: EmptyTuple]