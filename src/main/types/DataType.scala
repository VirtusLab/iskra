package org.virtuslab.iskra
package types

sealed trait DataType

object DataType:
  type Subtype[T <: DataType] = T

  sealed trait NotNull extends DataType

  type NumericType = ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType
  type NumericOptType = ByteOptType | ShortOptType | IntegerOptType | LongOptType | FloatOptType | DoubleOptType

  type Nullable[T <: DataType] <: DataType = T match
    case BooleanOptType => BooleanOptType
    case StringOptType => StringOptType
    case ByteOptType => ByteOptType
    case ShortOptType => ShortOptType
    case IntegerOptType => IntegerOptType
    case LongOptType => LongOptType
    case FloatOptType => FloatOptType
    case DoubleOptType => DoubleOptType
    case StructOptType[schema] => StructOptType[schema]

  type NonNullable[T <: DataType] <: DataType = T match
    case BooleanOptType => BooleanType
    case StringOptType => StringType
    case ByteOptType => ByteType
    case ShortOptType => ShortOptType
    case IntegerOptType => IntegerOptType
    case LongOptType => LongOptType
    case FloatOptType => FloatOptType
    case DoubleOptType => DoubleOptType
    case StructOptType[schema] => StructOptType[schema]

  type CommonNumericNullableType[T1 <: DataType, T2 <: DataType] <: NumericOptType = (T1, T2) match
    case (DoubleOptType, _) | (_, DoubleOptType) => DoubleOptType
    case (FloatOptType, _) | (_, FloatOptType) => FloatOptType
    case (LongOptType, _) | (_, LongOptType) => LongOptType
    case (IntegerOptType, _) | (_, IntegerOptType) => IntegerOptType
    case (ShortOptType, _) | (_, ShortOptType) => ShortOptType
    case (ByteOptType, _) | (_, ByteOptType) => ByteOptType

  type CommonNumericNonNullableType[T1 <: DataType, T2 <: DataType] <: NumericOptType = (T1, T2) match
    case (DoubleOptType, _) | (_, DoubleOptType) => DoubleType
    case (FloatOptType, _) | (_, FloatOptType) => FloatType
    case (LongOptType, _) | (_, LongOptType) => LongType
    case (IntegerOptType, _) | (_, IntegerOptType) => IntegerType
    case (ShortOptType, _) | (_, ShortOptType) => ShortType
    case (ByteOptType, _) | (_, ByteOptType) => ByteType

import DataType.NotNull

sealed class BooleanOptType extends DataType
final class BooleanType extends BooleanOptType, NotNull

sealed class StringOptType extends DataType
final class StringType extends StringOptType, NotNull

sealed class ByteOptType extends DataType
final class ByteType extends ByteOptType, NotNull

sealed class ShortOptType extends DataType
final class ShortType extends ShortOptType, NotNull

sealed class IntegerOptType extends DataType
final class IntegerType extends IntegerOptType, NotNull

sealed class LongOptType extends DataType
final class LongType extends LongOptType, NotNull

sealed class FloatOptType extends DataType
final class FloatType extends FloatOptType, NotNull

class DoubleOptType extends DataType
final class DoubleType extends DoubleOptType, NotNull

sealed class StructOptType[Schema <: Tuple] extends DataType
final class StructType[Schema <: Tuple] extends StructOptType[Schema], NotNull
