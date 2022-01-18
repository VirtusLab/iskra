package org.virtuslab.typedframes

extension [S <: TableSchema](inline tdf: TypedDataFrame[S])
  inline def show() = tdf.untyped.show()