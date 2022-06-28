package org.virtuslab.typedframes

trait SparkOpCtx:
  type CtxOut
  def ctxOut: CtxOut

// $ will have different members depending on the context,
// e.g. names of columns or data frame aliases that can be currenly referenced
transparent inline def $(using ctx: SparkOpCtx): ctx.CtxOut = ctx.ctxOut