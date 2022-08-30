package org.virtuslab.iskra.functions

import org.apache.spark.sql
import org.virtuslab.iskra.Column
import org.virtuslab.iskra.types.DataType.PrimitiveEncoder

def lit[A](value: A)(using encoder: PrimitiveEncoder[A]): Column[encoder.ColumnType] = Column(sql.functions.lit(encoder.encode(value)))
