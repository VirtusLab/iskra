package org.virtuslab.iskra

import scala.language.implicitConversions

import org.virtuslab.iskra.types.StructEncoder
import org.virtuslab.iskra.frame.DataFrameMacroOps
import org.virtuslab.iskra.frame.DataFrameMethods

class DataFrame[Schema <: StructSchema](val untyped: UntypedDataFrame) extends DataFrameMethods[Schema] {
  type Alias
}

object DataFrame /* extends DataFrameExtensions */ {
  // class WithSchema[Schema <: StructSchema](val untyped: UntypedDataFrame) {
  //   type Alias
  // }
  // object WithSchema extends DataFrameExtensions

  class Of[A](val untyped: UntypedDataFrame)
  
  object Of {
    implicit def withSchema[A](df: DataFrame.Of[A])(implicit e: StructEncoder[A]): DataFrame[e.Schema] =
      new DataFrame[e.Schema](df.untyped)
  }

  implicit def DataFrameMacroOps[S <: StructSchema](df: DataFrame[S]): DataFrameMacroOps[S] = new DataFrameMacroOps(df)
}

