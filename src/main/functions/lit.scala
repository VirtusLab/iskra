package org.virtuslab.typedframes.functions

import org.apache.spark.sql
import org.virtuslab.typedframes.Column
import org.virtuslab.typedframes.types.DataType.PrimitiveEncoder

def lit[A](value: A)(using encoder: PrimitiveEncoder[A]): Column[encoder.ColumnType] = Column(sql.functions.lit(encoder.encode(value)))
