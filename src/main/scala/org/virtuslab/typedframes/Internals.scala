package org.virtuslab.typedframes

private object Internals:
  type Name = String & Singleton
  type NameLike[T <: Name] = T