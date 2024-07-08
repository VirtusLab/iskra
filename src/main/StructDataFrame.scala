package org.virtuslab.iskra

import scala.quoted.*

import types.{DataType, Encoder, StructEncoder}


class StructDataFrame[Schema](val untyped: UntypedDataFrame) extends DataFrame

object StructDataFrame:
  type Subtype[T <: StructDataFrame[?]] = T
  type WithAlias[T <: String & Singleton] = StructDataFrame[?] { type Alias = T }

  extension [Schema](df: StructDataFrame[Schema])
    inline def asClass[A]: ClassDataFrame[A] = ${ asClassImpl[Schema, A]('df) }

  private def asClassImpl[FrameSchema : Type, A : Type](df: Expr[StructDataFrame[FrameSchema]])(using Quotes): Expr[ClassDataFrame[A]] =
    import quotes.reflect.report

    Expr.summon[Encoder[A]] match
      case Some(encoder) => encoder match
        case '{ $enc: StructEncoder[A] { type StructSchema = structSchema } } =>
          Type.of[MacroHelpers.AsTuple[FrameSchema]] match
            case '[`structSchema`] =>
              '{ ClassDataFrame[A](${ df }.untyped) }
            case _ =>
              val frameColumns = allColumns(Type.of[FrameSchema])
              val structColumns = allColumns(Type.of[structSchema])
              val errorMsg = s"A structural data frame with columns:\n${showColumns(frameColumns)}\nis not equivalent to a class data frame of ${Type.show[A]}, which would be encoded as a row with columns:\n${showColumns(structColumns)}"
              quotes.reflect.report.errorAndAbort(errorMsg)
        case '{ $enc: Encoder[A] { type ColumnType = colType } } =>
          def fromDataType[T : Type] =
            Type.of[T] match
              case '[`colType`] =>
                '{ ClassDataFrame[A](${ df }.untyped) }
              case '[t] =>
                val frameColumns = allColumns(Type.of[FrameSchema])
                val errorMsg = s"A structural data frame with columns:\n${showColumns(frameColumns)}\nis not equivalent to a class data frame of ${Type.show[A]}"
                quotes.reflect.report.errorAndAbort(errorMsg)
          Type.of[FrameSchema] match
            case '[label := dataType] =>
              fromDataType[dataType]
            case '[(label := dataType) *: EmptyTuple] =>
              fromDataType[dataType]
            case '[t] =>
              val frameColumns = allColumns(Type.of[FrameSchema])
              val errorMsg = s"A structural data frame with columns:\n${showColumns(frameColumns)}\nis not equivalent to a class data frame of ${Type.show[A]}"
              quotes.reflect.report.errorAndAbort(errorMsg)
      case None => report.errorAndAbort(s"Could not summon encoder for ${Type.show[A]}")

  private def allColumns(schemaType: Type[?])(using Quotes): Seq[Type[?]] =
    schemaType match
      case '[prefix / suffix := dataType] => Seq(Type.of[suffix := dataType])
      case '[Name.Subtype[suffix] := dataType] => Seq(Type.of[suffix := dataType])
      case '[EmptyTuple] => Seq.empty
      case '[head *: tail] => allColumns(Type.of[head]) ++ allColumns(Type.of[tail])

  private def showColumns(columnsTypes: Seq[Type[?]])(using Quotes): String =
    val columns = columnsTypes.map {
      case '[label := dataType] =>
        val shortDataType = Type.show[dataType].split("\\.").last
        s"${Type.show[label]} := ${shortDataType}"
    }
    columns.mkString(", ")
