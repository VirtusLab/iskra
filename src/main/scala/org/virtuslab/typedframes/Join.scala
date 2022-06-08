package org.virtuslab.typedframes

// import scala.quoted.*
import Internals.Name
import org.virtuslab.typedframes.types.StructType

enum JoinType:
  case Inner
  case Left
  case Right
  case Outer
  // TODO: Add other join types (treating Cross join separately?)

/* abstract */ class JoinOps[S1 <: StructType](leftFrame: TypedDataFrame[S1]):
  // type SelectionViewLeft <: SelectionView
  // type AllColumnsLeft <: Tuple
  // val leftView: SelectionViewLeft

  def apply[S2 <: StructType](rightFrame: TypedDataFrame[S2], joinType: JoinType = JoinType.Inner)(using svp1: SelectionView.Provider[S1], svp2: SelectionView.Provider[S2]) =
    new ConditionalJoiner(leftFrame, rightFrame, joinType) {
      // type SelectionViewLeft = JoinOps.this.SelectionViewLeft
      type SelectionViewLeft = svp1.View
      type SelectionViewRight = svp2.View
      // type AllColumnsLeft = JoinOps.this.AllColumnsLeft
      type AllColumnsLeft = svp1.view.AllColumns
      type AllColumnsRight = svp2.view.AllColumns
      // val leftView: SelectionViewLeft = JoinOps.this.leftView
      val leftView: SelectionViewLeft = svp1.view
      val rightView: SelectionViewRight = svp2.view
    }
  // def inner[S2 <: StructType](rightFrame: TypedDataFrame[S2])(using svp2: SelectionView.Provider[S2]) = ConditionalJoiner(leftFrame, rightFrame, JoinType.Inner)
  // def left[S2 <: StructType](rightFrame: TypedDataFrame[S2])(using SelectionView.Provider[S2]) = ConditionalJoiner(leftFrame, rightFrame, JoinType.Left)
  // def right[S2 <: StructType](rightFrame: TypedDataFrame[S2])(using SelectionView.Provider[S2]) = ConditionalJoiner(leftFrame, rightFrame, JoinType.Right)
  // def outer[S2 <: StructType](rightFrame: TypedDataFrame[S2])(using SelectionView.Provider[S2]) = ConditionalJoiner(leftFrame, rightFrame, JoinType.Outer)

extension [S <: StructType](inline tdf: TypedDataFrame[S])
  inline def join/* (using svp: SelectionView.Provider[S]) */ = new JoinOps(tdf) {
    // type SelectionViewLeft = svp.View
    // type AllColumnsLeft = svp.view.AllColumns
    // val leftView = svp.view
  }



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