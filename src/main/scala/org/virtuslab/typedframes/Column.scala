package org.virtuslab.typedframes

import org.apache.spark.sql.{ Column => UntypedColumn}
import types.DataType

class TypedColumn[+T <: DataType](val untyped: UntypedColumn) /* extends AnyVal */:
  inline def as[N <: Name](name: N)(using v: ValueOf[N]): LabeledColumn[N, T] =
    LabeledColumn[N, T](untyped.as(v.value))

  inline def name(using v: ValueOf[Name]): Name = v.value

object TypedColumn:
  given untypedColumnOps: {} with
    extension (untyped: UntypedColumn)
      def typed[A <: DataType] = TypedColumn[A](untyped)

class LabeledColumn[L <: LabeledColumn.Label, +T <: DataType](untyped: UntypedColumn) extends TypedColumn[T](untyped)

object LabeledColumn:
  type Label = Name | (Name, Name)
