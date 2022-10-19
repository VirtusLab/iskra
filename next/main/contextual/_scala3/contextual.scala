//> using target.scala "3"

package org.virtuslab.iskra

import scala.language.implicitConversions

import org.virtuslab.iskra.frame.SchemaView

package object contextual {
  inline def $(using view: SchemaView): view.type = view

  implicit def toDataFrameContextualOps[Schema <: StructSchema](df: DataFrame[Schema]): DataFrameContextualOps[Schema] = new DataFrameContextualOps(df)
}