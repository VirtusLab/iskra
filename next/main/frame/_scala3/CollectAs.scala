//> using target.scala "3"

package org.virtuslab.iskra
package frame

import scala.quoted.*
import org.virtuslab.iskra.types.{Encoder, StructEncoder}

object CollectAs:
  def collectAsImpl[Schema <: StructSchema : Type, A : Type](df: Expr[DataFrame[Schema]])(using Quotes): Expr[List[A]] =
    Expr.summon[Encoder[A]] match
      case Some(encoder) => encoder match
        case '{ $enc: StructEncoder[A] { type StructSchema = structSchema } } =>
          Type.of[MacroHelpers.AsTuple[Schema]] match
            case '[`structSchema`] =>
              '{ ${ df }.untyped.collect.toList.map(row => ${ enc }.decode(row).asInstanceOf[A]) }
            case _ =>
              val frameColumns = allColumns(Type.of[Schema])
              val structColumns = allColumns(Type.of[structSchema])
              val errorMsg = s"A data frame with columns:\n${showColumns(frameColumns)}\ncannot be collected as a list of ${Type.show[A]}, which would be encoded as a row with columns:\n${showColumns(structColumns)}"
              quotes.reflect.report.errorAndAbort(errorMsg)
        case '{ $enc: Encoder[A] { type ColumnType = colType } } =>
          def fromDataType[T : Type] =
            Type.of[T] match
              case '[`colType`] =>
                '{ ${ df }.untyped.collect.toList.map(row => ${ enc }.decode(row(0)).asInstanceOf[A]) }
              case '[t] =>
                val frameColumns = allColumns(Type.of[Schema])
                val errorMsg = s"A data frame with columns:\n${showColumns(frameColumns)}\ncannot be collected as a list of ${Type.show[A]}"
                quotes.reflect.report.errorAndAbort(errorMsg)
          Type.of[Schema] match
            case '[label := dataType] =>
              fromDataType[dataType]
            case '[(label := dataType) *: EmptyTuple] =>
              fromDataType[dataType]
      case _ => quotes.reflect.report.errorAndAbort(s"Could not summon encoder for ${Type.show[A]}")
    
  private def allColumns(schemaType: Type[?])(using Quotes): Seq[Type[?]] =
    schemaType match
      case '[prefix / suffix := dataType] => Seq(Type.of[suffix := dataType])
      case '[Name.Subtype[suffix] := dataType] => Seq(Type.of[suffix := dataType])
      case '[EmptyTuple] => Seq.empty
      case '[head *: tail] => allColumns(Type.of[head]) ++ allColumns(Type.of[tail])

  private def showColumns(columnTypes: Seq[Type[?]])(using Quotes): String =
    val columns = columnTypes.map {
      case '[label := dataType] =>
        val shortDataType = Type.show[dataType].split("\\.").last
        s"${Type.show[label]} := ${shortDataType}"
    }
    columns.mkString(", ")