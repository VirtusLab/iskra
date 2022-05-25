package org.virtuslab.typedframes

extension [S <: FrameSchema](inline tdf: TypedDataFrame[S])
  inline def show() = tdf.untyped.show()