package org.virtuslab.typedframes

private object Internals:
  type Name = String & Singleton
  object Name:
    type Subtype[T <: Name] = T