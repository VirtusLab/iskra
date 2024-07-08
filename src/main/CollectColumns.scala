package org.virtuslab.iskra

import scala.compiletime.error

import org.virtuslab.iskra.types.DataType

// TODO should it be covariant or not?
trait CollectColumns[-C]:
  type CollectedColumns <: Tuple
  def underlyingColumns(c: C): Seq[UntypedColumn]

// Using `given ... with { ... }` syntax might sometimes break pattern match on  `CollectColumns[...] { type CollectedColumns = cc }`

object CollectColumns:
  given collectNamedColumn[N <: Name, T <: DataType]: CollectColumns[NamedColumn[N, T]] with
    type CollectedColumns = (N := T) *: EmptyTuple
    def underlyingColumns(c: NamedColumn[N, T]) = Seq(c.untyped)

  given collectColumnsWithSchema[S <: Tuple]: CollectColumns[ColumnsWithSchema[S]] with
    type CollectedColumns = S
    def underlyingColumns(c: ColumnsWithSchema[S]) = c.underlyingColumns

  given collectEmptyTuple[S]: CollectColumns[EmptyTuple] with
    type CollectedColumns = EmptyTuple
    def underlyingColumns(c: EmptyTuple) = Seq.empty

  given collectCons[H, T <: Tuple](using collectHead: CollectColumns[H], collectTail: CollectColumns[T]): (CollectColumns[H *: T] { type CollectedColumns = Tuple.Concat[collectHead.CollectedColumns, collectTail.CollectedColumns] }) =
    new CollectColumns[H *: T]:
      type CollectedColumns = Tuple.Concat[collectHead.CollectedColumns, collectTail.CollectedColumns]
      def underlyingColumns(c: H *: T) = collectHead.underlyingColumns(c.head) ++ collectTail.underlyingColumns(c.tail)


  // TODO Customize error message for different operations with an explanation
  class CannotCollectColumns(typeName: String)
    extends Exception(s"Could not find an instance of CollectColumns for ${typeName}")
