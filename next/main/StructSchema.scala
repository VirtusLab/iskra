package org.virtuslab.iskra

import annotation.showAsInfix
import types.DataType

sealed trait StructSchema

object StructSchema {
  type Subtype[T <: StructSchema] = T
  sealed trait Empty extends StructSchema
  sealed trait NonEmpty extends StructSchema
}

// trait Label

@showAsInfix
trait /[Prefix <: Name, Suffix <: Name]

@showAsInfix
sealed trait :=[Label, T <: DataType] extends StructSchema.NonEmpty

@showAsInfix
final class **[Init <: StructSchema, Last <: :=[_, _]] extends StructSchema.NonEmpty