package org.virtuslab.typedframes

import types.*
import Internals.Name

extension [N1 <: Name](col1: TypedColumn[N1, IntegerType])
  inline def +[N2 <: Name](col2: TypedColumn[N2, IntegerType]) = UnnamedTypedColumn[IntegerType](col1.underlying + col2.underlying)

// More operations can be added easily