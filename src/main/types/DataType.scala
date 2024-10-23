package org.virtuslab.iskra
package types

sealed trait Nullability

sealed trait Nullable extends Nullability
sealed trait NonNullable extends Nullability

trait NullableOf[T <: DataType & NonNullable] extends Nullable
trait NonNullableOf[T <: DataType & Nullable] extends NonNullable


trait DataType

abstract class FinalDataType extends DataType {
  self: Nullability => 
}

object DataType:
  type Subtype[T <: DataType] = T

  type AsNullable[T <: DataType] <: DataType = T match
    case NonNullableOf[t] => t
    case Nullable => T

  
trait BooleanOptLike extends DataType
trait BooleanLike extends BooleanOptLike
final class boolean_? extends FinalDataType, NullableOf[boolean], BooleanOptLike
final class boolean extends FinalDataType, NonNullableOf[boolean_?], BooleanLike
type BooleanOrNull = boolean_?
type BooleanNotNull = boolean

trait StringOptLike extends DataType
trait StringLike extends StringOptLike
final class string_? extends FinalDataType, NullableOf[string], StringOptLike
final class string extends FinalDataType, NonNullableOf[string_?], StringLike
type StringOrNull = string_?
type StringNotNull = string

trait DoubleOptLike extends DataType
trait DoubleLike extends DoubleOptLike
final class double_? extends FinalDataType, NullableOf[double], DoubleOptLike
final class double extends FinalDataType, NonNullableOf[double_?], DoubleLike
type DoubleOrNull = double_?
type DoubleNotNull = double

trait FloatOptLike extends DoubleOptLike
trait FloatLike extends FloatOptLike, DoubleLike
final class float_? extends FinalDataType, NullableOf[float], FloatOptLike
final class float extends FinalDataType, NonNullableOf[float_?], FloatLike
type FloatOrNull = float_?
type FloatNotNull = float

trait LongOptLike extends FloatOptLike
trait LongLike extends LongOptLike, FloatLike
final class long_? extends FinalDataType, NullableOf[long], LongOptLike
final class long extends FinalDataType, NonNullableOf[long_?], LongLike
type LongOrNull = long_?
type LongNotNull = long

trait IntOptLike extends LongOptLike
trait IntLike extends IntOptLike, LongLike
final class int_? extends FinalDataType, NullableOf[int], IntOptLike
final class int extends FinalDataType, NonNullableOf[int_?], IntLike
type IntOrNull = int_?
type IntNotNull = int

trait ShortOptLike extends IntOptLike
trait ShortLike extends ShortOptLike, IntLike
final class short_? extends FinalDataType, NullableOf[short], ShortOptLike
final class short extends FinalDataType, NonNullableOf[short_?], ShortLike
type ShortOrNull = short_?
type ShortNotNull = short

trait ByteOptLike extends ShortOptLike
trait ByteLike extends ByteOptLike, ShortLike
final class byte_? extends FinalDataType, NullableOf[byte], ByteOptLike
final class byte extends FinalDataType, NonNullableOf[byte_?], ByteLike
type ByteOrNull = byte_?
type ByteNotNull = byte

trait StructOptLike[Schema <: Tuple] extends DataType
trait StructLike[Schema <: Tuple] extends StructOptLike[Schema]
final class struct_?[Schema <: Tuple] extends FinalDataType, NullableOf[struct[Schema]], StructOptLike[Schema]
final class struct[Schema <: Tuple] extends FinalDataType, NonNullableOf[struct_?[Schema]], StructLike[Schema]
type StructOrNull[Schema <: Tuple] = struct_?[Schema]
type StructNotNull[Schema <: Tuple] = struct[Schema]

type CommonNumericType[T1 <: DataType, T2 <: DataType] <: DataType = (T1, T2) match
  case (ByteLike, ByteLike) => byte
  case (ByteOptLike, ByteOptLike) => byte_?
  case (ShortLike, ShortLike) => short
  case (ShortOptLike, ShortOptLike) => short_?
  case (IntLike, IntLike) => int
  case (IntOptLike, IntOptLike) => int_?
  case (LongLike, LongLike) => long
  case (LongOptLike, LongOptLike) => long_?
  case (FloatLike, FloatLike) => float
  case (FloatOptLike, FloatOptLike) => float_?
  case (DoubleLike, DoubleLike) => double
  case (DoubleOptLike, DoubleOptLike) => double_?

type CommonBooleanType[T1 <: DataType, T2 <: DataType] <: DataType = (T1, T2) match
  case (BooleanLike, BooleanLike) => BooleanNotNull
  case (BooleanOptLike, BooleanOptLike) => BooleanOrNull

type CommonNullability[T1 <: Nullability, T2 <: Nullability] <: Nullability = (T1, T2) match
  case (NonNullable, NonNullable) => NonNullable
  case _ => Nullable

type BooleanOfNullability[N <: Nullability] <: DataType = N match
  case NonNullable => BooleanNotNull
  case Nullable => BooleanOrNull

type BooleanOfCommonNullability[T1, T2] <: DataType = (T1, T2) match
  case (NonNullable, NonNullable) => BooleanNotNull
  case (Nullability, Nullability) => BooleanOrNull

type DoubleOfCommonNullability[T1 <: DoubleOptLike, T2 <: DoubleOptLike] <: DataType = (T1, T2) match
  case (DoubleLike, DoubleLike) => DoubleNotNull
  case (DoubleOptLike, DoubleOptLike) => DoubleOrNull

type StringOfCommonNullability[T1 <: StringOptLike, T2 <: StringOptLike] <: DataType = (T1, T2) match
  case (StringLike, StringLike) => StringNotNull
  case (StringOptLike, StringOptLike) => StringOrNull
