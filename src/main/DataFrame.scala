package org.virtuslab.iskra

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession


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

  given dataFrameOps: {} with
    extension (df: DataFrame)
      inline def show(): Unit = df.untyped.show()
