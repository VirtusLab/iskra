/* TODOs:
 * - keep ordering of columns in a frame (?)
 * - support joins
 * - structure code better
 * - simplify macros
 * - drop `Typed` prefix from names but assure everything is usable in a way that names don't get confused with untyped ones taken directly from Spark API
 * - distinguish between named and unnamed columns? Disallow multiple columns with the same name in a frame?
 */

package org.virtuslab.typedframes

//////////////////////


trait SparkOpCtx:
  type CtxOut
  def ctxOut: CtxOut

// TODO: Add other contexts as described by $

/* $ will have different members depending on the context
  - inside select: all columns of the dataframe we select from
  - when joining directly on columns: the common columns of both data frames
  - when joining on arbitrary conditions: ?
      maybe references to the schemas of all joined frames to allow disabiguation to allow writing code like
      ```
      df1.join(df2).on($.left.id === $.right.index)
      ```
      Alternatively it would be nice if something like
      ```
      df1.join(df2).on(df1.id === df2.index)
      ```
      or
      ```
      df1.join(df2).on($(df1).id === $(df2).index)
      ```
      also worked but this wouldn't be very useful if df1 or df2 weren't assigned to variables or had very long names
*/

transparent inline def $(using ctx: SparkOpCtx): ctx.CtxOut = ctx.ctxOut