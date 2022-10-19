package org.virtuslab.iskra
package types

import org.apache.spark.sql

trait Encoder[A] {
  type ColumnType <: DataType
  def encode(value: A): Any
  def decode(value: Any): Any
  def catalystType: sql.types.DataType
  def isNullable: Boolean
}

trait PrimitiveEncoder[A] extends Encoder[A]

trait PrimitiveNullableEncoder[A] extends PrimitiveEncoder[Option[A]] {
  def encode(value: Option[A]): Any = value.orNull
  def decode(value: Any): Any = Option(value)
  def isNullable = true
}

trait PrimitiveNonNullableEncoder[A] extends PrimitiveEncoder[A] {
  def encode(value: A): Any = value
  def decode(value: Any): Any = value
  def isNullable = true
}


object Encoder extends StructEncoderInstances {
  type Aux[A, E <: DataType] = Encoder[A] { type ColumnType = E }

  implicit object boolean extends PrimitiveNonNullableEncoder[Boolean] {
    type ColumnType = BooleanType
    def catalystType = sql.types.BooleanType
  }
  implicit object booleanOpt extends PrimitiveNullableEncoder[Boolean] {
    type ColumnType = BooleanOptType
    def catalystType = sql.types.BooleanType
  }

  implicit object string extends PrimitiveNonNullableEncoder[String] {
    type ColumnType = StringType
    def catalystType = sql.types.StringType
  }
  implicit object stringOpt extends PrimitiveNullableEncoder[String] {
    type ColumnType = StringOptType
    def catalystType = sql.types.StringType
  }

  implicit object byte extends PrimitiveNonNullableEncoder[Byte] {
    type ColumnType = ByteType
    def catalystType = sql.types.ByteType
  }
  implicit object byteOpt extends PrimitiveNullableEncoder[Byte] {
    type ColumnType = ByteOptType
    def catalystType = sql.types.ByteType
  }

  implicit object short extends PrimitiveNonNullableEncoder[Short] {
    type ColumnType = ShortType
    def catalystType = sql.types.ShortType
  }
  implicit object shortOpt extends PrimitiveNullableEncoder[Short] {
    type ColumnType = ShortOptType
    def catalystType = sql.types.ShortType
  }

  implicit object int extends PrimitiveNonNullableEncoder[Int] {
    type ColumnType = IntegerType
    def catalystType = sql.types.IntegerType
  }
  implicit object intOpt extends PrimitiveNullableEncoder[Int] {
    type ColumnType = IntegerOptType
    def catalystType = sql.types.IntegerType
  }

  implicit object long extends PrimitiveNonNullableEncoder[Long] {
    type ColumnType = LongType
    def catalystType = sql.types.LongType
  }
  implicit object longOpt extends PrimitiveNullableEncoder[Long] {
    type ColumnType = LongOptType
    def catalystType = sql.types.LongType
  }

  implicit object float extends PrimitiveNonNullableEncoder[Float] {
    type ColumnType = FloatType
    def catalystType = sql.types.FloatType
  }
  implicit object floatOpt extends PrimitiveNullableEncoder[Float] {
    type ColumnType = FloatOptType
    def catalystType = sql.types.FloatType
  }

  implicit object double extends PrimitiveNonNullableEncoder[Double] {
    type ColumnType = DoubleType
    def catalystType = sql.types.DoubleType
  }
  implicit object doubleOpt extends PrimitiveNullableEncoder[Double] {
    type ColumnType = DoubleOptType
    def catalystType = sql.types.DoubleType
  }
}

trait StructEncoder[A] extends Encoder[A] {
  type Schema <: StructSchema
  type ColumnType = StructType[Schema]
  override def catalystType: sql.types.StructType
  override def encode(a: A): sql.Row
}

object StructEncoder {
  type Aux[A, E <: StructSchema] = StructEncoder[A] { type Schema = E }
}
