package org.virtuslab.iskra

import scala.quoted.*
import scala.deriving.Mirror
import types.DataType
import MacroHelpers.TupleSubtype

object FrameSchema:
  type Merge[S1, S2] = S1 match
    case TupleSubtype[s1] => S2 match
      case TupleSubtype[s2] => Tuple.Concat[s1, s2]
      case _ => Tuple.Concat[s1, S2 *: EmptyTuple]
    case _ => S2 match
      case TupleSubtype[s2] => S1 *: s2
      case _ => S1 *: S2 *: EmptyTuple

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
