package org.virtuslab.typedframes

import Internals.Name
import org.virtuslab.typedframes.types.StructType

enum JoinType:
  case Inner
  case Left
  case Right
  case Outer
  // TODO: Add other join types (treating Cross join separately?)

class JoinSemiOps[DF1 <: TypedDataFrame[?]](leftFrame: DF1) extends AnyVal:
  def apply[DF2 <: TypedDataFrame[?]](rightFrame: DF2, joinType: JoinType = JoinType.Inner) =
    new JoinOps(leftFrame, rightFrame, joinType)

  def inner[DF2 <: TypedDataFrame[?]](rightFrame: DF2) = apply(rightFrame, JoinType.Inner)
  def left[DF2 <: TypedDataFrame[?]](rightFrame: DF2) = apply(rightFrame, JoinType.Left)
  def right[DF2 <: TypedDataFrame[?]](rightFrame: DF2) = apply(rightFrame, JoinType.Right)
  def outer[DF2 <: TypedDataFrame[?]](rightFrame: DF2) = apply(rightFrame, JoinType.Outer)

class JoinOps[DF1 <: TypedDataFrame[?], DF2 <: TypedDataFrame[?]](left: DF1, right: DF2, joinType: JoinType):
  transparent inline def on = ${ ConditionalJoiner.make[DF1, DF2]('left, 'right, 'joinType) }

extension [S <: StructType](/* inline */ tdf: TypedDataFrame[S])
  inline def join = new JoinSemiOps(tdf)



// class Joiner[S1 <: StructType, S2 <: StructType](left: TypedDataFrame[S1], right: TypedDataFrame[S2])

// extension [S1 <: StructType, S2 <: StructType](joiner: Joiner[S1, S2])(jvp: JoinView.Provider[S1, S2])
//   def on[T](f: JoinCtx { type CtxOut = svp.View } ?=> T)


// // trait SharedColumnsJoinCtx extends SparkOpCtx:
// //   type CtxOut <: TableSchema

// // extension [S <: TableSchema](inline tdf: TypedDataFrame[S])
// //     inline def join[S2 <: TableSchema](inline tdf2: TypedDataFrame[S2]) = new JoinOps(tdf, tdf2)

// // trait CommonColumns[S1 <: TableSchema, S2 <: TableSchema]:
// //   type Schema <: TableSchema

// // trait JoinSchemas[S1 <: TableSchema, S2 <: TableSchema, Columns]:
// //   type JoinedSchema <: TableSchema
// //   val usingColumns: Seq[String]

// // class JoinOps[S1 <: TableSchema, S2 <: TableSchema](df1: TypedDataFrame[S1], df2: TypedDataFrame[S2]): // TODO use opaque type?
// //   // join on single column
// //   inline def on[Columns](using cc: CommonColumns[S1, S2])(f: SharedColumnsJoinCtx { type CtxOut = cc.Schema } ?=> Columns)(using js: JoinSchemas[S1, S2, Columns]): TypedDataFrame[js.JoinedSchema] = 
// //     val ctx: SharedColumnsJoinCtx { type CtxOut = S1 & S2 } = new SharedColumnsJoinCtx {
// //       type CtxOut = S1 & S2
// //       def ctxOut: CtxOut = TableSchema().asInstanceOf[CtxOut]
// //     }

// //     df1.untyped.join(df2.untyped, js.usingColumns).withSchema[js.JoinedSchema]






// /////////////////////////


// // transparent inline given commonColumns[S1 <: TableSchema, S2 <: TableSchema]: CommonColumns[S1, S2] = ${ commonColumnsImpl[S1, S2] }

// //def commonColumnsImpl[S1 <: TableSchema : Type, S2 <: TableSchema : Type](using Quotes): Expr[CommonColumns[S1, S2]] = 
  

// // given [S1 <: TableSchema, S2 <: TableSchema, ColName <: Name]: JoinSchemas[S1, S2, ColName] with
// //   type JoinedSchema = 