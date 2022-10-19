package org.virtuslab.iskra
package types

sealed trait DataType

object DataType {
  type Subtype[T <: DataType] = T

  sealed trait NotNull extends DataType
}

import DataType.NotNull

sealed class BooleanOptType extends DataType
final class BooleanType extends BooleanOptType with NotNull

sealed class StringOptType extends DataType
final class StringType extends StringOptType with NotNull

sealed class ByteOptType extends DataType
final class ByteType extends ByteOptType with NotNull

sealed class ShortOptType extends DataType
final class ShortType extends ShortOptType with NotNull

sealed class IntegerOptType extends DataType
final class IntegerType extends IntegerOptType with NotNull

sealed class LongOptType extends DataType
final class LongType extends LongOptType with NotNull

sealed class FloatOptType extends DataType
final class FloatType extends FloatOptType with NotNull

class DoubleOptType extends DataType
final class DoubleType extends DoubleOptType with NotNull

sealed class StructOptType[Schema <: StructSchema] extends DataType
final class StructType[Schema <: StructSchema] extends StructOptType[Schema] with NotNull


// TODO: Missing types

trait IsDataType[-T]
object IsDataType {
  implicit object stringOpt extends IsDataType[StringOptType]
  implicit object integerOpt extends IsDataType[IntegerOptType]
  implicit object doubleOpt extends AsDoubleOpt[DoubleOptType]
}

// trait AsDouble[T]
trait AsDoubleOpt[-T]
object AsDoubleOpt {
  implicit object integerOpt extends AsDoubleOpt[IntegerOptType]
  implicit object doubleOpt extends AsDoubleOpt[DoubleOptType]
}

trait IsNumberOpt[-T]
object IsNumberOpt {
  implicit object integerOpt extends IsNumberOpt[IntegerOptType]
  implicit object doubleOpt extends IsNumberOpt[DoubleOptType]
}

trait CommonNumericType[-T1, -T2] {
  type Out
}

object CommonNumericType extends CommonNumericTypeLowPrio {
  implicit object integer extends CommonNumericType[IntegerType, IntegerType] {
    override type Out = IntegerType
  }
}

trait CommonNumericTypeLowPrio {
  implicit def integerOpt[
    T1 <: IntegerOptType, T2 <: IntegerOptType
  ]: CommonNumericType[IntegerOptType, IntegerOptType] { type Out = IntegerOptType } =
    new CommonNumericType[IntegerOptType, IntegerOptType] {
      override type Out = IntegerOptType
    }
}