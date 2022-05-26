package org.virtuslab.typedframes

import scala.quoted._
import scala.deriving.Mirror
import scala.compiletime.erasedValue
import Internals.{Name, NameLike}

sealed trait FrameSchema:
  //private def elems
  override def toString =
    val elems = FrameSchema.elems(this).map((label, typeName) => s"${label}: ${typeName}").mkString(", ")
    s"FrameSchema {${elems}}"


object FrameSchema:
  object SNil extends FrameSchema//:
  type SNil = SNil.type
  final case class SCons[N <: Name, H, T <: FrameSchema](headLabel: N, headTypeName: String, tail: T) extends FrameSchema//:
  private def elems(schema: FrameSchema): List[(String, String)] = schema match
    case SNil => Nil
    case SCons(headLabel, headTypeName, tail) => (headLabel, headTypeName) :: elems(tail)

  type FromLabelsAndTypes[Ls <: Tuple, Ts <: Tuple] <: FrameSchema = Ls match
    case NameLike[elemLabel] *: elemLabels => Ts match
      case elemType *: elemTypes =>
        SCons[elemLabel, elemType, FromLabelsAndTypes[elemLabels, elemTypes]]
    case EmptyTuple => SNil

  type WithSingleColumn[N <: Name, ColType] = FromLabelsAndTypes[N *: EmptyTuple, ColType *: EmptyTuple]

  trait Provider[A]:
    type Schema <: FrameSchema

  object Provider:
    transparent inline given fromMirror[A](using m: Mirror.ProductOf[A]): Provider[A] = new Provider[A]:
      type Schema = FromLabelsAndTypes[m.MirroredElemLabels, m.MirroredElemTypes]

  inline def of[A](using p: Provider[A]): p.Schema = instance[p.Schema]

  inline def instance[Schema <: FrameSchema]: Schema = inline erasedValue[Schema] match
    case _: SNil.type => SNil.asInstanceOf[Schema]
    case _: SCons[headLabel, headType, tail] =>
      (new SCons[headLabel, headType, tail](valueOf[headLabel], getTypeName[headType], instance[tail])).asInstanceOf[Schema]


  // TODO: Conformance should be recursive
  // trait Conformance[S1 <: FrameSchema, S2 <: FrameSchema]

  // object Conformance:
  //   inline given instance[S1 <: FrameSchema, S2 <: FrameSchema]: Conformance[S1, S2]

  // transparent inline def schemaInstance[Schema <: FrameSchema]: FrameSchema = erasedValue[Schema] match
  //   case _: SNil.type => SNil
  //   case _: SCons[headLabel, headType, tail] =>
  //     // println("!!!!!!")
  //     // println(headLabel)
  //     // println(headType)
  //     // println(tail)
  //     inline val tailSchema = schemaInstance[tail].asInstanceOf[tail]
  //     (new SCons[headLabel, headType, tail](valueOf[headLabel], "FakeType", tailSchema))//.asInstanceOf[Schema]

// inline def sconsSchemaInstance[headLabel <: Name, headType, tail <: FrameSchema]: Schema =
  
//   erasedValue[Schema] match