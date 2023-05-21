package org.virtuslab.iskra

import scala.quoted.*
import MacroHelpers.TupleSubtype

class WithColumns[Schema, View <: SchemaView](val view: View, val underlying: UntypedDataFrame)

object WithColumns:
  given dataFrameWithColumnsOps: {} with
    extension [Schema, DF <: StructDataFrame[Schema]](df: DF)
      transparent inline def withColumns: WithColumns[Schema, ?] = ${ withColumnsImpl[Schema, DF]('{df}) }

  def withColumnsImpl[Schema : Type, DF <: StructDataFrame[Schema] : Type](df: Expr[DF])(using Quotes): Expr[WithColumns[Schema, ?]] =
    import quotes.reflect.asTerm
    val viewExpr = StructSchemaView.schemaViewExpr[DF]
    viewExpr.asTerm.tpe.asType match
      case '[SchemaView.Subtype[v]] =>
        '{
          new WithColumns[Schema, v](
            view = ${ viewExpr }.asInstanceOf[v],
            underlying = ${ df }.untyped
          )
        }

  given withColumnsApply: {} with
    extension [Schema <: Tuple, View <: SchemaView](withColumns: WithColumns[Schema, View])
      transparent inline def apply(inline columns: View ?=> NamedColumns[?]*): StructDataFrame[?] =
        ${ applyImpl[Schema, View]('withColumns, 'columns) }

  def applyImpl[Schema <: Tuple : Type, View <: SchemaView : Type](
    withColumns: Expr[WithColumns[Schema, View]],
    columns: Expr[Seq[View ?=> NamedColumns[?]]]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*

    val columnValuesWithTypesWithLabels = columns match
      case Varargs(colExprs) =>
        colExprs.map { arg =>
          val reduced = Term.betaReduce('{$arg(using ${ withColumns }.view)}.asTerm).get
          reduced.asExpr match
            case '{ $value: NamedColumns[schema] } => ('{ ${ value }.underlyingColumns }, Type.of[schema], labelsNames(Type.of[schema]))
        }

    val columnsValues = columnValuesWithTypesWithLabels.map(_._1)
    val columnsTypes = columnValuesWithTypesWithLabels.map(_._2)
    val columnsNames = columnValuesWithTypesWithLabels.map(_._3).flatten

    val schemaTpe = FrameSchema.schemaTypeFromColumnsTypes(columnsTypes)
    schemaTpe match
      case '[TupleSubtype[s]] =>
        '{
          val cols = ${ Expr.ofSeq(columnsValues) }.flatten
          val withColumnsAppended =
            ${ Expr(columnsNames) }.zip(cols).foldLeft(${ withColumns }.underlying){
              case (df, (label, col)) =>
                df.withColumn(label, col)
            }
          StructDataFrame[Tuple.Concat[Schema, s]](withColumnsAppended)
        }

  private def labelsNames(schema: Type[?])(using Quotes): List[String] =
    schema match
      case '[EmptyTuple] => Nil
      case '[(label := column) *: tail] =>
        val headValue = Type.valueOfConstant[label].get.toString
        headValue :: labelsNames(Type.of[tail])
