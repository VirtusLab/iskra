// package org.virtuslab.typedframes

// import scala.quoted.*
// import Internals.Name

// trait SharedColumnsJoinCtx extends SparkOpCtx:
//   type CtxOut <: TableSchema

// extension [S <: TableSchema](inline tdf: TypedDataFrame[S])
//     inline def join[S2 <: TableSchema](inline tdf2: TypedDataFrame[S2]) = new JoinOps(tdf, tdf2)

// trait CommonColumns[S1 <: TableSchema, S2 <: TableSchema]:
//   type Schema <: TableSchema

// trait JoinSchemas[S1 <: TableSchema, S2 <: TableSchema, Columns]:
//   type JoinedSchema <: TableSchema
//   val usingColumns: Seq[String]

// class JoinOps[S1 <: TableSchema, S2 <: TableSchema](df1: TypedDataFrame[S1], df2: TypedDataFrame[S2]): // TODO use opaque type?
//   // join on single column
//   inline def on[Columns](using cc: CommonColumns[S1, S2])(f: SharedColumnsJoinCtx { type CtxOut = cc.Schema } ?=> Columns)(using js: JoinSchemas[S1, S2, Columns]): TypedDataFrame[js.JoinedSchema] = 
//     val ctx: SharedColumnsJoinCtx { type CtxOut = S1 & S2 } = new SharedColumnsJoinCtx {
//       type CtxOut = S1 & S2
//       def ctxOut: CtxOut = TableSchema().asInstanceOf[CtxOut]
//     }

//     df1.untyped.join(df2.untyped, js.usingColumns).withSchema[js.JoinedSchema]






/////////////////////////


// transparent inline given commonColumns[S1 <: TableSchema, S2 <: TableSchema]: CommonColumns[S1, S2] = ${ commonColumnsImpl[S1, S2] }

//def commonColumnsImpl[S1 <: TableSchema : Type, S2 <: TableSchema : Type](using Quotes): Expr[CommonColumns[S1, S2]] = 
  

// given [S1 <: TableSchema, S2 <: TableSchema, ColName <: Name]: JoinSchemas[S1, S2, ColName] with
//   type JoinedSchema = 