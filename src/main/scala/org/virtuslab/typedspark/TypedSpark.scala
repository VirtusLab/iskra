/* TODOs:
 * - keep ordering of columns in a frame (?)
 * - support joins
 * - structure code better
 * - simplify macros
 * - drop `Typed` prefix from names but assure everything is usable in a way that names don't get confused with untyped ones taken directly from Spark API
 */

package org.virtuslab.typedspark

import scala.util.NotGiven

import scala.deriving.Mirror
import org.apache.spark.sql.{ Column, DataFrame, DatasetHolder, Encoder, SparkSession }
import org.apache.spark.sql.functions.col
import scala.annotation.implicitNotFound

object TypedSpark:
  import scala.quoted.*

  private object Internals:
    type Name = String & Singleton

  import Internals.*

  object TypedColumnOpaqueScope:
    opaque type TypedColumn[N <: Name, T] = Column // Should be covariant for T?
    def NamedTypedColumn[N <: Name, T](underlying: Column): TypedColumn[N, T] = underlying
    def UnnamedTypedColumn[T](underlying: Column): TypedColumn[Nothing, T] = underlying
    extension [N <: Name, T](inline tc: TypedColumn[N, T])
      inline def underlying: Column = tc
      inline def named[NewName <: Name](using v: ValueOf[NewName]): TypedColumn[NewName, T] =
        NamedTypedColumn[NewName, T](underlying.as(v.value))
      inline def name(using v: ValueOf[N]): String = v.value

  export TypedColumnOpaqueScope.*

  class TableSchema extends Selectable:
    def selectDynamic(name: String): TypedColumn[Nothing, Any] =
      UnnamedTypedColumn[Any](col(name))

  trait Schema1For[A]:
    type Schema <: TableSchema

  trait SchemaFor[A]:
    type Schema <: TableSchema

  transparent inline given schemaFromMirror[A : Mirror.ProductOf]: SchemaFor[A] = ${schemaFromMirrorImpl[A]}

  def schemaFromMirrorImpl[A : Type](using Quotes): Expr[SchemaFor[A]] =
    productSchemaType[A].asType match
      case '[t] =>
        '{
          new SchemaFor[A] {
            type Schema = t
          }.asInstanceOf[SchemaFor[A] { type Schema = t }]
        }

  object TypedDataFrameOpaqueScope:
    opaque type TypedDataFrame[+S <: TableSchema] = DataFrame
    extension (inline df: DataFrame)
      inline def typed[A](using schema: SchemaFor[A]): TypedDataFrame[schema.Schema] = df
      inline def withSchema[S <: TableSchema]: TypedDataFrame[S] = df // TODO? make it private[TypedSpark]
    extension [A](inline seq: Seq[A])(using schema: SchemaFor[A])(using encoder: Encoder[A], spark: SparkSession)
      inline def toTypedDF: TypedDataFrame[schema.Schema] =
        import spark.implicits.*
        seq.toDF(/* Should we explicitly pass columns here? */)

    extension [A <: Int | String](inline seq: Seq[A])(using spark: SparkSession) // TODO: Add more primitive types
      // TODO decide on/unify naming
      transparent inline def toTypedDF[N <: Name]: TypedDataFrame[TableSchema] = ${toTypedDFWithNameImpl[N, A]('seq, 'spark)}
      transparent inline def toTypedDFNamed[N <: Name](columnName: N): TypedDataFrame[TableSchema] = seq.toTypedDF[N]

    private def toTypedDFWithNameImpl[N <: Name : Type, A : Type](using Quotes)(seq: Expr[Seq[A]], spark: Expr[SparkSession]): Expr[TypedDataFrame[TableSchema]] =
      '{
        val s = $spark
        given Encoder[A] = ${ Expr.summon[Encoder[A]].get }
        import s.implicits.*
        localSeqToDatasetHolder($seq).toDF(valueOf[N])
      }

    extension [S <: TableSchema](tdf: TypedDataFrame[S])
      inline def asDF: DataFrame = tdf

  export TypedDataFrameOpaqueScope.*


  extension [S <: TableSchema](inline tdf: TypedDataFrame[S])
    inline def show() = tdf.asDF.show()

  //////////////////////

  // Column ops

  extension [N1 <: Name](col1: TypedColumn[N1, Int])
    inline def +[N2 <: Name](col2: TypedColumn[N2, Int]) = UnnamedTypedColumn[Int](col1.underlying + col2.underlying)

  // More can be added easily

  //////////////////////

  trait SparkOpCtx:
    type CtxOut
    def ctxOut: CtxOut

  trait SelectCtx extends SparkOpCtx:
    type CtxOut <: TableSchema

  // TODO: Add other contexts as described by $


  trait MergeColumns[T]:
    type MergedSchema <: TableSchema
    def columns(t: T): List[Column]

  type ColumnsNames[T <: Tuple] <: Tuple = T match
    case TypedColumn[name, tpe] *: tail => name *: ColumnsNames[tail]
    case EmptyTuple => EmptyTuple

  type ColumnsTypes[T <: Tuple] <: Tuple = T match
    case TypedColumn[name, tpe] *: tail => tpe *: ColumnsTypes[tail]
    case EmptyTuple => EmptyTuple

  transparent inline given mergeTupleColumns[T <: Tuple]: MergeColumns[T] = ${ mergeTupleColumnsImpl[T] }

  def mergeTupleColumnsImpl[T <: Tuple : Type](using Quotes): Expr[MergeColumns[T]] = 
    schemaTypeWithColumns[ColumnsNames[T], ColumnsTypes[T]].asType match
      case '[s] =>
        '{
          new MergeColumns[T] {
            type MergedSchema = s
            def columns(t: T): List[Column] = t.toList.map(col => col.asInstanceOf[TypedColumn[Name, Any]].underlying)
          }.asInstanceOf[MergeColumns[T] {type MergedSchema = s}]
        }

  // TODO: can/should we somehow reuse the implementation for tuples?
  transparent inline given [N <: Name, A]: MergeColumns[TypedColumn[N, A]] = ${ mergeSingleColumnImpl[N, A] }

  def mergeSingleColumnImpl[N <: Name : Type, A : Type](using Quotes): Expr[MergeColumns[TypedColumn[N, A]]] =
    val colName = Expr(Type.valueOfConstant[N].get.toString)
    schemaTypeWithColumns[N *: EmptyTuple, A *: EmptyTuple].asType match
      case '[s] =>
        '{
          new MergeColumns[TypedColumn[N, A]] {
            type MergedSchema = s
            def columns(t: TypedColumn[N, A]): List[Column] = List(t.underlying)
          }.asInstanceOf[MergeColumns[TypedColumn[N, A]] {type MergedSchema = s}]
        }


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

  transparent inline def $(using inline ctx: SparkOpCtx): ctx.CtxOut = ctx.ctxOut


  extension [S <: TableSchema](inline tdf: TypedDataFrame[S])
    inline def select[T](f: SelectCtx { type CtxOut = S } ?=> T)(using mc: MergeColumns[T]): TypedDataFrame[mc.MergedSchema] =
      val ctx: SelectCtx { type CtxOut = S } = new SelectCtx {
        type CtxOut = S
        def ctxOut: S = TableSchema().asInstanceOf[S]
      }
      val typedCols = f(using ctx)
      val columns  = mc.columns(typedCols)
      (tdf.asDF.select(columns*)).withSchema[mc.MergedSchema]


  transparent inline def schemaOfProduct[T]: TableSchema = ${ schemaOfProductImpl[T] }

  private def productSchemaType[T : Type](using Quotes): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[T]].get match
      case '{ $m: Mirror.ProductOf[T] {type MirroredElemLabels = mels; type MirroredElemTypes = mets } } =>
        schemaTypeWithColumns[mels, mets]

  private def makeSchemaInstance(using Quotes)(tp: Type[?]): Expr[TableSchema] =
    tp match
      case '[tpe] =>
        val res = '{
          val p = TableSchema()
          p.asInstanceOf[TableSchema & tpe]
        }
        res

  private def schemaOfProductImpl[T : Type](using Quotes): Expr[TableSchema] = 
    val tpe = productSchemaType[T]
    makeSchemaInstance(tpe.asType)

  type NameLike[T <: Name] = T

  private def schemaTypeWithColumns(using Quotes)(elementLabels: quotes.reflect.TypeRepr, elementTypes: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    refineWithColumns(TypeRepr.of[TableSchema], elementLabels, elementTypes)

  private def schemaTypeWithColumns[Labels : Type, Types : Type](using Quotes): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    schemaTypeWithColumns(TypeRepr.of[Labels], TypeRepr.of[Types])

  private def refineWithColumns(using Quotes)(base: quotes.reflect.TypeRepr, elementLabels: quotes.reflect.TypeRepr, elementTypes: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr =
    import quotes.reflect.*

    elementLabels.asType match
      case '[NameLike[elemLabel] *: elemLabels] =>
        val label = Type.valueOfConstant[elemLabel].get.toString
        elementTypes.asType match
          case '[elemType *: elemTypes] =>
            val info = TypeRepr.of[TypedColumn[elemLabel, elemType]]
            val newBase = Refinement(base, label, info)
            refineWithColumns(newBase, TypeRepr.of[elemLabels], TypeRepr.of[elemTypes])
      case '[EmptyTuple] => base