package org.virtuslab.iskra

import scala.quoted.*
import scala.deriving.Mirror
import types.{DataType, Encoder, StructEncoder}
import MacroHelpers.TupleSubtype

object FrameSchema:
  type AsTuple[A] = A match
    case Tuple => A
    case Any => A *: EmptyTuple

  type FromTuple[T] = T match
    case h *: EmptyTuple => h
    case Tuple => T

  type Merge[S1, S2] = (S1, S2) match
    case (Tuple, Tuple) =>
      Tuple.Concat[S1, S2]
    case (Any, Tuple) =>
      S1 *: S2
    case (Tuple, Any) =>
      Tuple.Append[S1, S2]
    case (Any, Any) =>
      (S1, S2)

  type NullableLabeledDataType[T] = T match
    case label := tpe => label := DataType.Nullable[tpe]

  type NullableSchema[T] = T match
    case Tuple => Tuple.Map[T, NullableLabeledDataType]
    case Any => NullableLabeledDataType[T]

  def reownType[Owner <: Name : Type](schema: Type[?])(using Quotes): Type[?] =
    schema match
      case '[EmptyTuple] => Type.of[EmptyTuple]
      case '[head *: tail] =>
        reownType[Owner](Type.of[head]) match
          case '[reownedHead] =>
            reownType[Owner](Type.of[tail]) match
              case '[TupleSubtype[reownedTail]] =>
                Type.of[reownedHead *: reownedTail]
      case '[namePrefix / nameSuffix := dataType] =>
        Type.of[Owner / nameSuffix := dataType]
      case '[Name.Subtype[name] := dataType] =>
        Type.of[Owner / name := dataType]

  def isValidType(tpe: Type[?])(using Quotes): Boolean = tpe match
    case '[EmptyTuple] => true
    case '[(label := column) *: tail] => isValidType(Type.of[tail])
    case '[label := column] => true
    case _ => false

  def schemaTypeFromColumnsTypes(colTypes: Seq[Type[?]])(using Quotes): Type[? <: Tuple] =
    colTypes match
      case Nil => Type.of[EmptyTuple]
      case '[TupleSubtype[headTpes]] :: tail =>
        schemaTypeFromColumnsTypes(tail) match
        case '[TupleSubtype[tailTpes]] => Type.of[Tuple.Concat[headTpes, tailTpes]]
