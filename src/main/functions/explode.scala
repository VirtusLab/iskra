package org.virtuslab.iskra.functions

import org.apache.spark.sql
import org.virtuslab.iskra.Column
import org.virtuslab.iskra.types.{ ArrayOptType, DataType }

def explode[T <: DataType](c: Column[ArrayOptType[T]]): Column[T] = Column(sql.functions.explode(c.untyped))

