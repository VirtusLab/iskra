package org.virtuslab.typedframes

import scala.quoted._
import scala.deriving.Mirror
import scala.compiletime.erasedValue
// import org.apache.spark.sql.functions.col
import Internals.Name

sealed trait FrameSchema

object FrameSchema:
  object SNil extends FrameSchema
  type SNil = SNil.type

  final case class SCons[N <: Name, H, T <: FrameSchema](headLabel: N, headTypeName: Name, tail: T) extends FrameSchema

  // def of[A : FrameSchemaFor] = summon[FrameSchemaFor[A]].schema

  type WithSingleColumn[N <: Name, ColType] = FrameSchemaFromLabelsAndTypes[N *: EmptyTuple, ColType *: EmptyTuple]

import FrameSchema.{ SNil, SCons }

trait FrameSchemaFor[A]:
  type Schema <: FrameSchema
  // def schema: Schema

type FrameSchemaFromLabelsAndTypes[Ls <: Tuple, Ts <: Tuple] <: FrameSchema = Ls match
  case NameLike[elemLabel] *: elemLabels => Ts match
    case elemType *: elemTypes =>
      FrameSchema.SCons[elemLabel, elemType, FrameSchemaFromLabelsAndTypes[elemLabels, elemTypes]]
  case EmptyTuple => FrameSchema.SNil

// inline def schemaInstance[Schema <: FrameSchema]: Schema = erasedValue[Schema] match
//   case _: SNil.type => SNil.asInstanceOf[Schema]
//   case _: SCons[headLabel, headType, tail] =>
//     // println("!!!!!!")
//     // println(headLabel)
//     // println(headType)
//     // println(tail)
//     (new SCons[headLabel, headType, tail](valueOf[headLabel], "FakeType", schemaInstance[tail])).asInstanceOf[Schema]

transparent inline given frameSchemaFromMirror[A](using m: Mirror.ProductOf[A]): FrameSchemaFor[A] = new FrameSchemaFor[A]:
  type Schema = FrameSchemaFromLabelsAndTypes[m.MirroredElemLabels, m.MirroredElemTypes]
  // def schema = schemaInstance[Schema]

type NameLike[T <: Name] = T