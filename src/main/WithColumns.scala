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
    extension [Schema, View <: SchemaView](withColumns: WithColumns[Schema, View])
      transparent inline def apply[Cols](columns: View ?=> Cols): StructDataFrame[?] =
        ${ applyImpl[Schema, View, Cols]('withColumns, 'columns) }

  def applyImpl[Schema : Type, View <: SchemaView : Type, Cols : Type](
    withColumns: Expr[WithColumns[Schema, View]],
    columns: Expr[View ?=> Cols]
  )(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.*
    Type.of[Cols] match
      case '[name := colType] =>
        val label = Expr(Type.valueOfConstant[name].get.toString)
        '{
          type OutSchema = FrameSchema.Merge[Schema, name := colType]
          val col = ${ columns }(using ${ withColumns }.view).asInstanceOf[Column[?]].untyped
          StructDataFrame[OutSchema](
            ${ withColumns }.underlying.withColumn(${label}, col)
          )
        }
        
      case '[TupleSubtype[s]] if FrameSchema.isValidType(Type.of[s]) =>
        val labels = Expr.ofSeq(labelsNames(Type.of[s]))
        '{
          type OutSchema = FrameSchema.Merge[Schema, s]
          val cols: Seq[UntypedColumn] = ${ columns }(using ${ withColumns }.view).asInstanceOf[s].toList.map(_.asInstanceOf[Column[?]].untyped)
          val withColumnsAppended =
            ${labels}.zip(cols).foldLeft(${ withColumns }.underlying){
              case (df, (label, col)) =>
                df.withColumn(label, col)
            }
          StructDataFrame[OutSchema](withColumnsAppended)
        }

      case '[t] =>
        val errorMsg = s"""The parameter of `withColumns` should be a named column (e.g. of type: "foo" := StringType) or a tuple of named columns but it has type: ${Type.show[t]}"""
        report.errorAndAbort(errorMsg, MacroHelpers.callPosition(withColumns))

  private def labelsNames(schema: Type[?])(using Quotes): List[Expr[String]] =
    schema match
      case '[EmptyTuple] => Nil
      case '[(label := column) *: tail] =>
        val headValue = Expr(Type.valueOfConstant[label].get.toString)
        headValue :: labelsNames(Type.of[tail])
