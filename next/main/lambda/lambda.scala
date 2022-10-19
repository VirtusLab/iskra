package org.virtuslab.iskra

import scala.language.implicitConversions

package object lambda {
  implicit def toDataFrameLambdaOps[Schema <: StructSchema](df: DataFrame[Schema]): DataFrameLambdaOps[Schema] = new DataFrameLambdaOps(df)
}