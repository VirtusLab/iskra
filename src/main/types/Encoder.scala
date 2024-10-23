package org.virtuslab.iskra
package types

import scala.quoted._
import scala.deriving.Mirror
import org.apache.spark.sql
import MacroHelpers.TupleSubtype


trait Encoder[-A]:
  type ColumnType <: DataType
  def encode(value: A): Any
  def decode(value: Any): Any
  def catalystType: sql.types.DataType
  def isNullable: Boolean

trait PrimitiveEncoder[-A] extends Encoder[A]

trait PrimitiveNullableEncoder[-A] extends PrimitiveEncoder[Option[A]]:
  def encode(value: Option[A]) = value.orNull
  def decode(value: Any) = Option(value)
  def isNullable = true

trait PrimitiveNonNullableEncoder[-A] extends PrimitiveEncoder[A]:
  def encode(value: A) = value
  def decode(value: Any) = value
  def isNullable = false


object Encoder:
  type Aux[-A, E <: DataType] = Encoder[A] { type ColumnType = E }

  inline given booleanEncoder: PrimitiveNonNullableEncoder[Boolean] with
    type ColumnType = BooleanNotNull
    def catalystType = sql.types.BooleanType
  inline given booleanOptEncoder: PrimitiveNullableEncoder[Boolean] with
    type ColumnType = BooleanOrNull
    def catalystType = sql.types.BooleanType

  inline given stringEncoder: PrimitiveNonNullableEncoder[String] with
    type ColumnType = StringNotNull
    def catalystType = sql.types.StringType
  inline given stringOptEncoder: PrimitiveNullableEncoder[String] with
    type ColumnType = StringOrNull
    def catalystType = sql.types.StringType

  inline given byteEncoder: PrimitiveNonNullableEncoder[Byte] with
    type ColumnType = ByteNotNull
    def catalystType = sql.types.ByteType
  inline given byteOptEncoder: PrimitiveNullableEncoder[Byte] with
    type ColumnType = ByteOrNull
    def catalystType = sql.types.ByteType

  inline given shortEncoder: PrimitiveNonNullableEncoder[Short] with
    type ColumnType = ShortNotNull
    def catalystType = sql.types.ShortType
  inline given shortOptEncoder: PrimitiveNullableEncoder[Short] with
    type ColumnType = ShortOrNull
    def catalystType = sql.types.ShortType

  inline given intEncoder: PrimitiveNonNullableEncoder[Int] with
    type ColumnType = IntNotNull
    def catalystType = sql.types.IntegerType
  inline given intOptEncoder: PrimitiveNullableEncoder[Int] with
    type ColumnType = IntOrNull
    def catalystType = sql.types.IntegerType

  inline given longEncoder: PrimitiveNonNullableEncoder[Long] with
    type ColumnType = LongNotNull
    def catalystType = sql.types.LongType
  inline given longOptEncoder: PrimitiveNullableEncoder[Long] with
    type ColumnType = LongOrNull
    def catalystType = sql.types.LongType

  inline given floatEncoder: PrimitiveNonNullableEncoder[Float] with
    type ColumnType = FloatNotNull
    def catalystType = sql.types.FloatType
  inline given floatOptEncoder: PrimitiveNullableEncoder[Float] with
    type ColumnType = FloatOrNull
    def catalystType = sql.types.FloatType

  inline given doubleEncoder: PrimitiveNonNullableEncoder[Double] with
    type ColumnType = DoubleNotNull
    def catalystType = sql.types.DoubleType
  inline given doubleOptEncoder: PrimitiveNullableEncoder[Double] with
    type ColumnType = DoubleOrNull
    def catalystType = sql.types.DoubleType

  export StructEncoder.{fromMirror, optFromMirror}

trait StructEncoder[-A] extends Encoder[A]:
  type StructSchema <: Tuple
  type ColumnType = StructNotNull[StructSchema]
  override def catalystType: sql.types.StructType
  override def encode(a: A): sql.Row

object StructEncoder:
  type Aux[-A, E <: Tuple] = StructEncoder[A] { type StructSchema = E }

  private case class ColumnInfo(
    labelType: Type[?],
    labelValue: String,
    decodedType: Type[?],
    encoder: Expr[Encoder[?]]
  )

  private def getColumnSchemaType(using quotes: Quotes)(subcolumnInfos: List[ColumnInfo]): Type[?] =
    subcolumnInfos match
      case Nil => Type.of[EmptyTuple]
      case info :: tail =>
        info.labelType match
          case '[Name.Subtype[lt]] =>
            info.encoder match
              case '{ ${encoder}: Encoder.Aux[tpe, DataType.Subtype[e]] } => 
                getColumnSchemaType(tail) match
                  case '[TupleSubtype[tailType]] => 
                    Type.of[(lt := e) *: tailType]

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

  transparent inline given fromMirror[A]: StructEncoder[A] = ${ fromMirrorImpl[A] }

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
          case '[TupleSubtype[t]] =>  
            '{
              (new StructEncoder[A] {
                override type StructSchema = t
                override def catalystType = sql.types.StructType(${ structFields })
                override def isNullable = false
                override def encode(a: A) =
                  sql.Row.fromSeq(${ Expr.ofSeq(rowElements('a)) })
                override def decode(a: Any) =
                  ${mirror}.fromProduct(${ rowElementsTuple('{a.asInstanceOf[sql.Row]}) })
              }): StructEncoder[A] { type StructSchema = t }
            }
  end fromMirrorImpl

  given optFromMirror[A](using encoder: StructEncoder[A]): (Encoder[Option[A]] { type ColumnType = StructOrNull[encoder.StructSchema] }) =
    new Encoder[Option[A]]:
      override type ColumnType = StructOrNull[encoder.StructSchema]
      override def encode(value: Option[A]): Any = value.map(encoder.encode).orNull
      override def decode(value: Any): Any = Option(encoder.decode)
      override def catalystType = encoder.catalystType
      override def isNullable = true
