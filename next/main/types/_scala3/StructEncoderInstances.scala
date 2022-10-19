//> using target.scala "3"

package org.virtuslab.iskra
package types

import scala.quoted.*
import scala.deriving.Mirror
import org.apache.spark.sql
import MacroHelpers.TupleSubtype

private[iskra] object StructEncoderInstances:
  private case class ColumnInfo(
    labelType: Type[?],
    labelValue: String,
    decodedType: Type[?],
    encoder: Expr[Encoder[?]]
  )

  // private def getColumnSchemaType(using quotes: Quotes)(subcolumnInfos: List[ColumnInfo]): Type[?] =
  //   subcolumnInfos match
  //     case Nil => Type.of[StructSchema.Empty]
  //     case info :: tail =>
  //       info.labelType match
  //         case '[Name.Subtype[lt]] =>
  //           info.encoder match
  //             case '{ ${encoder}: Encoder.Aux[tpe, DataType.Subtype[e]] } =>
  //               tail match
  //                 case Nil =>
  //                   Type.of[lt := e]
  //                 case _ => 
  //                   getColumnSchemaType(tail) match
  //                     case '[TupleSubtype[tailType]] => 
  //                       Type.of[tailType ** (lt := e)]

  private def getColumnSchemaType(using quotes: Quotes)(subcolumnInfos: List[ColumnInfo]): Type[?] =
    subcolumnInfos match
      case Nil => Type.of[StructSchema.Empty]
      case _ => getColumnSchemaTypeNonEmpty(subcolumnInfos.reverse)

  private def getColumnSchemaTypeNonEmpty(using quotes: Quotes)(subcolumnInfos: List[ColumnInfo]): Type[?] =
    val head = subcolumnInfos.head
    val tail = subcolumnInfos.tail
    head.labelType match
      case '[Name.Subtype[lt]] =>
        head.encoder match
          case '{ ${encoder}: Encoder.Aux[tpe, DataType.Subtype[e]] } =>
            type headType = lt := e
            tail match
              case Nil =>
                Type.of[headType]
              case _ =>
                getColumnSchemaTypeNonEmpty(tail) match
                  case '[StructSchema.Subtype[tailType]] => 
                    Type.of[tailType ** headType]

  private def getSubcolumnInfos(elemLabels: Type[?], elemTypes: Type[?])(using Quotes): List[ColumnInfo] =
    import quotes.reflect.Select
    elemLabels match
      case '[EmptyTuple] => Nil
      case '[label *: labels] =>
        val labelValue = Type.valueOfConstant[label].get.toString
        elemTypes match
          case '[tpe *: types] =>
            Expr.summon[Encoder[tpe]] match
              case Some(encoderExpr) =>
                ColumnInfo(Type.of[label], labelValue, Type.of[tpe], encoderExpr) :: getSubcolumnInfos(Type.of[labels], Type.of[types])
              case _ => quotes.reflect.report.errorAndAbort(s"Could not summon encoder for ${Type.show[tpe]}")

  def fromMirrorImpl[A : Type](using q: Quotes): Expr[StructEncoder[A]] =
    Expr.summon[Mirror.Of[A]].getOrElse(throw new Exception(s"Could not find Mirror when generating encoder for ${Type.show[A]}")) match
      case '{ ${mirror}: Mirror.ProductOf[A] { type MirroredElemLabels = elementLabels; type MirroredElemTypes = elementTypes } } =>
        val subcolumnInfos = getSubcolumnInfos(Type.of[elementLabels], Type.of[elementTypes])
        val columnSchemaType = getColumnSchemaType(subcolumnInfos)

        val structFieldExprs = subcolumnInfos.map { info =>
          '{ sql.types.StructField(${Expr(info.labelValue)}, ${info.encoder}.catalystType, ${info.encoder}.isNullable) }
        }
        val structFields = Expr.ofSeq(structFieldExprs)

        def rowElements(value: Expr[A]) =
          subcolumnInfos.map { info =>
            import quotes.reflect.*
            info.decodedType match
              case '[t] =>
                '{ ${info.encoder}.asInstanceOf[Encoder[t]].encode(${ Select.unique(value.asTerm, info.labelValue).asExprOf[t] }) }
          }

        def rowElementsTuple(row: Expr[sql.Row]): Expr[Tuple] =
          val elements = subcolumnInfos.zipWithIndex.map { (info, idx) =>
            given Quotes = q
            '{ ${info.encoder}.decode(${row}.get(${Expr(idx)})) }
          }
          Expr.ofTupleFromSeq(elements)

        columnSchemaType match
          case '[StructSchema.Subtype[t]] =>  
            '{
              (new StructEncoder[A] {
                override type Schema = t
                override def catalystType = sql.types.StructType(${ structFields })
                override def isNullable = false
                override def encode(a: A) =
                  sql.Row.fromSeq(${ Expr.ofSeq(rowElements('a)) })
                override def decode(a: Any) =
                  ${mirror}.fromProduct(${ rowElementsTuple('{a.asInstanceOf[sql.Row]}) })
              }): StructEncoder[A] { type Schema = t }
            }
  end fromMirrorImpl

import StructEncoderInstances.*

trait StructEncoderInstances:
  transparent inline given fromMirror[A]: StructEncoder[A] = ${ fromMirrorImpl[A] }

  inline given optFromMirror[A](using encoder: StructEncoder[A]): (Encoder[Option[A]] { type ColumnType = StructOptType[encoder.Schema] }) =
    new Encoder[Option[A]]:
      override type ColumnType = StructOptType[encoder.Schema]
      override def encode(value: Option[A]): Any = value.map(encoder.encode).orNull
      override def decode(value: Any): Any = Option(encoder.decode)
      override def catalystType = encoder.catalystType
      override def isNullable = true


      

// trait EncoderInstances:
//   inline given boolean: PrimitiveNonNullableEncoder[Boolean] with
//     type ColumnType = BooleanType
//     def catalystType = sql.types.BooleanType
//   inline given booleanOpt: PrimitiveNullableEncoder[Boolean] with
//     type ColumnType = BooleanOptType
//     def catalystType = sql.types.BooleanType

//   inline given string: PrimitiveNonNullableEncoder[String] with
//     type ColumnType = StringType
//     def catalystType = sql.types.StringType
//   inline given stringOpt: PrimitiveNullableEncoder[String] with
//     type ColumnType = StringOptType
//     def catalystType = sql.types.StringType

//   inline given byte: PrimitiveNonNullableEncoder[Byte] with
//     type ColumnType = ByteType
//     def catalystType = sql.types.ByteType
//   inline given byteOpt: PrimitiveNullableEncoder[Byte] with
//     type ColumnType = ByteOptType
//     def catalystType = sql.types.ByteType

//   inline given short: PrimitiveNonNullableEncoder[Short] with
//     type ColumnType = ShortType
//     def catalystType = sql.types.ShortType
//   inline given shortOpt: PrimitiveNullableEncoder[Short] with
//     type ColumnType = ShortOptType
//     def catalystType = sql.types.ShortType

//   inline given int: PrimitiveNonNullableEncoder[Int] with
//     type ColumnType = IntegerType
//     def catalystType = sql.types.IntegerType
//   inline given intOpt: PrimitiveNullableEncoder[Int] with
//     type ColumnType = IntegerOptType
//     def catalystType = sql.types.IntegerType

//   inline given long: PrimitiveNonNullableEncoder[Long] with
//     type ColumnType = LongType
//     def catalystType = sql.types.LongType
//   inline given longOpt: PrimitiveNullableEncoder[Long] with
//     type ColumnType = LongOptType
//     def catalystType = sql.types.LongType

//   inline given float: PrimitiveNonNullableEncoder[Float] with
//     type ColumnType = FloatType
//     def catalystType = sql.types.FloatType
//   inline given floatOpt: PrimitiveNullableEncoder[Float] with
//     type ColumnType = FloatOptType
//     def catalystType = sql.types.FloatType

//   inline given double: PrimitiveNonNullableEncoder[Double] with
//     type ColumnType = DoubleType
//     def catalystType = sql.types.DoubleType
//   inline given doubleOpt: PrimitiveNullableEncoder[Double] with
//     type ColumnType = DoubleOptType
//     def catalystType = sql.types.DoubleType
