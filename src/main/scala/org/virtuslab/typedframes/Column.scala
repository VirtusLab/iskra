package org.virtuslab.typedframes

import org.apache.spark.sql.{ Column => UntypedColumn}
import types.DataType
import Internals.Name

class TypedColumn[+T <: DataType](val untyped: UntypedColumn) /* extends AnyVal */:
  inline def named[N1 <: Internals.Name](name: N1)(using v: ValueOf[N1]): LabeledColumn[N1, T] =
    LabeledColumn[N1, T](untyped.as(v.value))

  private[typedframes] inline def namedAs[N1 <: Internals.Name](using v: ValueOf[N1]): LabeledColumn[N1, T] =
    LabeledColumn[N1, T](untyped.as(v.value))

  inline def name(using v: ValueOf[Name]): Name = v.value

given untypedColumnOps: {} with
  extension (untyped: UntypedColumn)
    def typed[A <: DataType] = TypedColumn[A](untyped)

type ColumnLabel = Name | (Name, Name)

object LabeledColumnOpaqueScope:
  class LabeledColumn[L <: ColumnLabel, +T <: DataType](untyped: UntypedColumn) extends TypedColumn[T](untyped)

  object LabeledColumn:
    def escapeColumnName(name: String) = s"`${name}`"

export LabeledColumnOpaqueScope.LabeledColumn
