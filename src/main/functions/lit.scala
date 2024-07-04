package org.virtuslab.iskra.functions

import org.apache.spark.sql
import org.virtuslab.iskra.Col
import org.virtuslab.iskra.types.PrimitiveEncoder

def lit[A](value: A)(using encoder: PrimitiveEncoder[A]): Col[encoder.ColumnType] = Col(sql.functions.lit(encoder.encode(value)))
