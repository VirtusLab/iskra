package org.virtuslab.typedframes

import org.apache.spark.sql.{ Column => UntypedColumn}
import Internals.Name

object TypedColumnOpaqueScope:
  opaque type TypedColumn[N <: Name, T] = UntypedColumn // Should be covariant for T?
  def NamedTypedColumn[N <: Name, T](underlying: UntypedColumn): TypedColumn[N, T] = underlying
  def UnnamedTypedColumn[T](underlying: UntypedColumn): TypedColumn[Nothing, T] = underlying
  extension [N <: Name, T](inline tc: TypedColumn[N, T])
    inline def underlying: UntypedColumn = tc
    inline def named[NewName <: Name](using v: ValueOf[NewName]): TypedColumn[NewName, T] =
      NamedTypedColumn[NewName, T](underlying.as(v.value))
    inline def name(using v: ValueOf[N]): String = v.value

export TypedColumnOpaqueScope.*