package org.virtuslab.iskra

import org.virtuslab.iskra.types.DataType

type AsColumnOrError[T] <: ColumnOrError = T match
  case DataType => Col[T]
  case _ => ColumnError

type ColumnFun1[C1 <: Column,  F[_ <: DataType]] <: ColumnOrError = C1 match
  case Col[t1] => AsColumnOrError[F[t1]]
  case _ => ColumnError


type ColumnFun2[C1 <: Column, C2 <: Column,  F[_ <: DataType, _ <: DataType]] <: ColumnOrError = (C1, C2) match
  case (Col[t1], Col[t2]) => AsColumnOrError[F[t1, t2]]
  case _ => ColumnError
