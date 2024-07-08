package org.virtuslab.iskra

import scala.quoted.*
import scala.deriving.Mirror
import types.{DataType, Encoder, StructEncoder}
import MacroHelpers.TupleSubtype

object FrameSchema:
  type Merge[S1, S2] = S1 match
    case TupleSubtype[s1] => S2 match
      case TupleSubtype[s2] => Tuple.Concat[s1, s2]
      case _ => Tuple.Concat[s1, S2 *: EmptyTuple]
    case _ => S2 match
      case TupleSubtype[s2] => S1 *: s2
      case _ => S1 *: S2 *: EmptyTuple

  type NullableLabeledDataType[T] = T match
    case label := tpe => label := DataType.Nullable[tpe]

  type NullableSchema[T] = T match
    case TupleSubtype[s] => Tuple.Map[s, NullableLabeledDataType]
    case _ => NullableLabeledDataType[T]

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
