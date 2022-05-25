package org.virtuslab.typedframes

import org.apache.spark.sql.{ Column => UntypedColumn}
import Internals.Name

object TypedColumnOpaqueScope:
  opaque type TypedColumn[N <: Name, T] = UntypedColumn // Should be covariant for T?

  def NamedTypedColumn[N <: Name, T](underlying: UntypedColumn): TypedColumn[N, T] = underlying
  def UnnamedTypedColumn[T](underlying: UntypedColumn): TypedColumn[Nothing, T] = underlying
  extension [N <: Name, T](inline tc: TypedColumn[N, T])
    inline def underlying: UntypedColumn = tc
    inline def named[N1 <: Name](using v: ValueOf[N1]): TypedColumn[N1, T] =
      NamedTypedColumn[N1, T](underlying.as(v.value))
    inline def named[N1 <: Name](name: N1)(using v: ValueOf[N1]): TypedColumn[N1, T] = named[N1]
    inline def name(using v: ValueOf[N]): N = v.value

export TypedColumnOpaqueScope.*