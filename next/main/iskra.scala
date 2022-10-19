package org.virtuslab

import scala.language.implicitConversions

import org.virtuslab.iskra.types.DataType

package object iskra extends ApiCommons {
  type Name = String with Singleton
  object Name {
    type Subtype[T <: Name] = T
    //TODO: Reverse Scala's mangling of operators, e.g. currectly the name for `a!` is `a$bang`
    def escape(name: String) = s"`${name}`"
  }

  type UntypedDataFrame = org.apache.spark.sql.DataFrame
  type UntypedColumn = org.apache.spark.sql.Column
  type UntypedRelationalGroupedDataset = org.apache.spark.sql.RelationalGroupedDataset

  type StringOptType = types.StringOptType
  type StringType = types.StringType
  type IntegerOptType = types.IntegerOptType
  type IntegerType = types.IntegerType
  type DoubleOptType = types.DoubleOptType
  type DoubleType = types.DoubleType
  // TODO remaining types

  @annotation.showAsInfix
  type As[C <: Column, N <: Name] = C with Alias[N]
  object As {
    def apply[N <: Name](col: Column): col.type As N = col.asInstanceOf[As[col.type, N]]
  }

  type SparkSession = org.apache.spark.sql.SparkSession
  val SparkSession = org.apache.spark.sql.SparkSession

  // implicit def DataFrameTypingOps(df: UntypedDataFrame) = new DataFrameTypingOps(df)
  // implicit def SeqTypingOps[A](seq: Seq[A])(implicit encoder: Encoder[A]): SeqTypingOps[A, encoder.type] = new SeqTypingOps(seq)

  // @annotation.showAsInfix
  // trait :=[N <: Name, T <: DataType]
}