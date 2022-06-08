package org.virtuslab.typedframes

import org.virtuslab.typedframes.types.{BooleanType, StructType}
import org.apache.spark.sql.catalyst.plans

// object ConditionalJoiner:
  // def make[S1 <: StructType, S2 <: StructType](
  //   left: TypedDataFrame[S1], right: TypedDataFrame[S2], joinType: JoinType
  // )

abstract class ConditionalJoiner[S1 <: StructType, S2 <: StructType](
  left: TypedDataFrame[S1], right: TypedDataFrame[S2], joinType: JoinType
)/* (using svp1: SelectionView.Provider[S1], svp2: SelectionView.Provider[S2]) */:
  type SelectionViewLeft <: SelectionView
  type SelectionViewRight <: SelectionView
  // type AllColumnsLeft <: Tuple
  // type AllColumnsRight <: Tuple
  val leftView: SelectionViewLeft
  val rightView: SelectionViewRight
  def on(f: (SelectionViewLeft, SelectionViewRight) => TypedColumn[BooleanType])
        (using mc: MergeColumns[Tuple.Concat[leftView.AllColumns, rightView.AllColumns/* AllColumnsLeft, AllColumnsRight */]]): TypedDataFrame[mc.MergedSchema] =
    
    val condition = f(leftView, rightView).untyped

    val sparkJoinType = joinType match
      case JoinType.Inner => "inner"
      case JoinType.Left => "left"
      case JoinType.Right => "right"
      case JoinType.Outer => "outer"

    left.untyped.join(right.untyped, condition, sparkJoinType).withSchema[mc.MergedSchema]
