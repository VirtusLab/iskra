package org.virtuslab.iskra

import types._

trait Plus[-C1, -C2] {
  type Result
  def apply(col1: C1, col2: C2): Result
}

object Plus extends PlusLowPrio {
  implicit def numeric[T1, C1[_] <: Column, T2, C2[_] <: Column](implicit
    cnt: CommonNumericType[T1, T2],
    ck: CommonKind[C1, C2]
  ): Plus[C1[T1], C2[T2]] { type Result = ck.Out[cnt.Out] } =
    new Plus[C1[T1], C2[T2]] {
      type Result = ck.Out[cnt.Out]
      def apply(col1: C1[T1], col2: C2[T2]): Result = ck.wrap(col1.untyped + col2.untyped)
    }
}

trait PlusLowPrio {
  // implicit object integerOpt extends Plus[IntegerOptType, IntegerOptType] {
  //   type Out = IntegerOptType
  // }
}


// trait Plus[-T1, -T2] {
//   type Out <: DataType
// }

// object Plus extends PlusLowPrio {
//   implicit object integer extends Plus[IntegerType, IntegerType] {
//     type Out = IntegerType
//   }
// }

// trait PlusLowPrio {
//   implicit object integerOpt extends Plus[IntegerOptType, IntegerOptType] {
//     type Out = IntegerOptType
//   }
// }