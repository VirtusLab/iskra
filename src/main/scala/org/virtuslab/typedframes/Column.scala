package org.virtuslab.typedframes

import org.apache.spark.sql.{ Column => UntypedColumn}
import types.DataType
import Internals.Name

object TypedColumnOpaqueScope:
  class TypedColumn[+T <: DataType](val untyped: UntypedColumn) extends AnyVal:
    type Name <: Internals.Name

    inline def named[N1 <: Internals.Name](name: N1)(using v: ValueOf[N1]): TypedColumn[T] { type Name = N1 } =
      TypedColumn[T](untyped.as(v.value)).asInstanceOf[TypedColumn[T] { type Name = N1 }]

    private[typedframes] inline def namedAs[N1 <: Internals.Name](using v: ValueOf[N1]): TypedColumn[T] { type Name = N1 } =
      TypedColumn[T](untyped.as(v.value)).asInstanceOf[TypedColumn[T] { type Name = N1 }]

    inline def name(using v: ValueOf[Name]): Name = v.value

  given untypedColumnOps: {} with
    extension (untyped: UntypedColumn)
      def typed[A <: DataType] = TypedColumn[A](untyped)

export TypedColumnOpaqueScope.{TypedColumn, untypedColumnOps}

object NamedColumnOpaqueScope:
  type NamedColumn[N <: Internals.Name, +T <: DataType] = TypedColumn[T] { type Name = N }

  object NamedColumn:
    def escapeColumnName(name: String) = s"`${name}`"

export NamedColumnOpaqueScope.NamedColumn
