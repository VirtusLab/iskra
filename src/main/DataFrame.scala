package org.virtuslab.typedframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import scala.quoted.*
import types.{DataType, StructType}

class DataFrame[Schema](val untyped: UntypedDataFrame):
  type Alias <: Name

object DataFrame:
  export Select.dataFrameSelectOps
  export Join.dataFrameJoinOps
  export GroupBy.dataFrameGroupByOps

  type Subtype[T <: DataFrame[?]] = T
  type WithAlias[T <: String & Singleton] = DataFrame[?] { type Alias = T }

  extension [S](tdf: DataFrame[S])
    transparent inline def as(inline frameName: Name): DataFrame[?] = ${ aliasImpl('tdf, 'frameName) }
    transparent inline def alias(inline frameName: Name): DataFrame[?] = ${ aliasImpl('tdf, 'frameName) }

  private def aliasImpl[S : Type](tdf: Expr[DataFrame[S]], frameName: Expr[String])(using Quotes) =
    import quotes.reflect.*
    ConstantType(StringConstant(frameName.valueOrAbort)).asType match
      case '[Name.Subtype[n]] =>
        FrameSchema.reownType[n](Type.of[S]) match
          case '[reowned] => '{
            new DataFrame[reowned](${ tdf }.untyped.alias(${ frameName })):
              type Alias = n
          }

  given dataFrameOps: {} with
    extension [S](tdf: DataFrame[S])
      inline def show(): Unit = tdf.untyped.show()

    extension [S](tdf: DataFrame[S])
      inline def collectAs[A]: List[A] =
        ${ collectAsImpl[S, A]('tdf) }

  private type AsTuple[A] = A match
    case Tuple => A
    case _ => A *: EmptyTuple

  // TODO: Use only a subset of columns 
  private def collectAsImpl[FrameSchema : Type, A : Type](df: Expr[DataFrame[FrameSchema]])(using Quotes): Expr[List[A]] =
    Expr.summon[DataType.Encoder[A]] match
      case Some(encoder) => encoder match
        case '{ $enc: DataType.StructEncoder[A] { type StructSchema = structSchema } } =>
          Type.of[MacroHelpers.AsTuple[FrameSchema]] match
            case '[`structSchema`] =>
              '{ ${ df }.untyped.collect.toList.map(row => ${ enc }.decode(row).asInstanceOf[A]) }
            case x =>
              val frameColumns = allColumns(Type.of[FrameSchema])
              val structColumns = allColumns(Type.of[structSchema])
              val errorMsg = s"A data frame with columns:\n${showColumns(frameColumns)}\ncannot be collected as a list of ${Type.show[A]}, which would be encoded as a row with columns:\n${showColumns(structColumns)}"
              quotes.reflect.report.errorAndAbort(errorMsg)
        case '{ $enc: DataType.Encoder[A] { type ColumnType = colType } } =>
          def fromDataType[T : Type] =
            Type.of[T] match
              case '[`colType`] =>
                '{ ${ df }.untyped.collect.toList.map(row => ${ enc }.decode(row(0)).asInstanceOf[A]) }
              case '[t] =>
                val frameColumns = allColumns(Type.of[FrameSchema])
                val errorMsg = s"A data frame with columns:\n${showColumns(frameColumns)}\ncannot be collected as a list of ${Type.show[A]}"
                quotes.reflect.report.errorAndAbort(errorMsg)
          Type.of[FrameSchema] match
            case '[label := dataType] =>
              fromDataType[dataType]
            case '[(label := dataType) *: EmptyTuple] =>
              fromDataType[dataType]
      case _ => quotes.reflect.report.throwError(s"Could not summon encoder for ${Type.show[A]}")
    
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
