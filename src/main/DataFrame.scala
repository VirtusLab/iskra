package org.virtuslab.iskra

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.virtuslab.iskra.types.Encoder


trait DataFrame:
  type Alias
  def untyped: UntypedDataFrame

object DataFrame:
  export Aliasing.dataFrameAliasingOps
  export Select.dataFrameSelectOps
  export Join.dataFrameJoinOps
  export GroupBy.dataFrameGroupByOps
  export Where.dataFrameWhereOps
  export WithColumns.dataFrameWithColumnsOps


  // Has to be inline - otherwise for an unknown reason the runtime and compile-time schemas don't match 
  inline def apply[A](elements: A*)(using encoder: Encoder[A])(using spark: SparkSession): ClassDataFrame[A] =
    DataFrameBuilders.toDF(elements)

  given dataFrameOps: {} with
    extension (df: DataFrame)
      inline def show(): Unit = df.untyped.show()
