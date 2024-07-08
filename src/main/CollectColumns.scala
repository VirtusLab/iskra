package org.virtuslab.iskra

import scala.compiletime.error

// TODO should it be covariant or not?
trait CollectColumns[-C]:
  type CollectedColumns <: Tuple
  def underlyingColumns(c: C): Seq[UntypedColumn]

// Using `given ... with { ... }` syntax might sometimes break pattern match on  `CollectColumns[...] { type CollectedColumns = cc }`

object CollectColumns extends CollectColumnsLowPrio:
  given collectSingle[S <: Tuple]: CollectColumns[NamedColumns[S]] with
    type CollectedColumns = S
    def underlyingColumns(c: NamedColumns[S]) = c.underlyingColumns

  given collectEmptyTuple[S]: CollectColumns[EmptyTuple] with
    type CollectedColumns = EmptyTuple
    def underlyingColumns(c: EmptyTuple) = Seq.empty

  given collectMultiCons[S <: Tuple, T <: Tuple](using collectTail: CollectColumns[T]): (CollectColumns[NamedColumns[S] *: T] { type CollectedColumns = Tuple.Concat[S, collectTail.CollectedColumns] }) =
    new CollectColumns[NamedColumns[S] *: T]:
      type CollectedColumns = Tuple.Concat[S, collectTail.CollectedColumns]
      def underlyingColumns(c: NamedColumns[S] *: T) = c.head.underlyingColumns ++ collectTail.underlyingColumns(c.tail)

  // TODO Customize error message for different operations with an explanation
  class CannotCollectColumns(typeName: String)
    extends Exception(s"Could not find an instance of CollectColumns for ${typeName}")


trait CollectColumnsLowPrio:
  given collectSingleCons[S, T <: Tuple](using collectTail: CollectColumns[T]): (CollectColumns[NamedColumns[S] *: T] { type CollectedColumns = S *: collectTail.CollectedColumns}) =
    new CollectColumns[NamedColumns[S] *: T]:
      type CollectedColumns = S *: collectTail.CollectedColumns
      def underlyingColumns(c: NamedColumns[S] *: T) = c.head.underlyingColumns ++ collectTail.underlyingColumns(c.tail)
