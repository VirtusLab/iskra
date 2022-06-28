package org.virtuslab.typedframes

type Name = String & Singleton
object Name:
  type Subtype[T <: Name] = T

  def escape(name: String) = s"`${name}`"