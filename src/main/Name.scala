package org.virtuslab.iskra

type Name = String & Singleton
object Name:
  type Subtype[T <: Name] = T

  //TODO: Reverse Scala's mangling of operators, e.g. currectly the name for `a!` is `a$bang`
  def escape(name: String) = s"`${name}`"