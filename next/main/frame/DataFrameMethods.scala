package org.virtuslab.iskra
package frame

private[iskra] trait DataFrameMethods[Schema <: StructSchema] { self: DataFrame[Schema] =>
  def show() = untyped.show()
}