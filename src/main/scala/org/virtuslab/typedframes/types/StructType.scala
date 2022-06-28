package org.virtuslab.typedframes.types

import org.virtuslab.typedframes.Name

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