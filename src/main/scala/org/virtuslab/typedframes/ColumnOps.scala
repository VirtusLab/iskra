package org.virtuslab.typedframes

import Internals.Name

extension [N1 <: Name](col1: TypedColumn[N1, Int])
  inline def +[N2 <: Name](col2: TypedColumn[N2, Int]) = UnnamedTypedColumn[Int](col1.underlying + col2.underlying)

// More operations can be added easily