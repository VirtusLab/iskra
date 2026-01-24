package org.virtuslab.iskra

import scala.quoted.*

import org.apache.spark.sql.{Column => UntypedColumn}
import types.DataType
import MacroHelpers.TupleSubtype

sealed class Column(val untyped: UntypedColumn) extends ColumnOps

object Column:
  def unsafeTyped[R <: ColumnOrError](untyped: UntypedColumn): R = new ColumnImpl(untyped).asInstanceOf[R]

  extension [T <: DataType](col: Col[T])
    inline def as[N <: Name](name: N): NamedColumn[N, T] =
      NamedColumn[N, T](col.untyped.as(name))
    inline def alias[N <: Name](name: N): NamedColumn[N, T] =
      NamedColumn[N, T](col.untyped.as(name))

  export ColumnOps.*
  export ColumnOps.given

final class DataColumn(untyped: UntypedColumn) extends Column(untyped)


// TODO: Get rid of covariance?
sealed class Col[+T <: DataType](untyped: UntypedColumn) extends Column(untyped)

final class ColumnImpl(untyped: UntypedColumn) extends Col[Nothing](untyped)

object ColumnImpl:
  def apply[R <: ColumnOrError](untyped: UntypedColumn): R = new ColumnImpl(untyped).asInstanceOf[R]


object Columns:
  transparent inline def apply[C <: NamedColumns](columns: C): ColumnsWithSchema[?] = ${ applyImpl('columns) }

  private def applyImpl[C : Type](columns: Expr[C])(using Quotes): Expr[ColumnsWithSchema[?]] =
    import quotes.reflect.*

    Expr.summon[CollectColumns[C]] match
      case Some(collectColumns) =>
        collectColumns match
          case '{ $cc: CollectColumns[?] { type CollectedColumns = collectedColumns } } =>
            Type.of[collectedColumns] match
              case '[TupleSubtype[collectedCols]] =>
                '{
                  val cols = ${ cc }.underlyingColumns(${ columns })
                  ColumnsWithSchema[collectedCols](cols)
                }
      case None =>
        // TODO: What should be returned here for best error messages?
        report.errorAndAbort("Cannot collect columns: " + Type.show[C])

type ColumnOrError = Column | ColumnError

sealed trait ColumnError

object ColumnError extends ColumnError
