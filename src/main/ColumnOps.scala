package org.virtuslab.iskra

import scala.quoted.*
import org.apache.spark.sql.functions.concat
import org.virtuslab.iskra.types.*
import DataType.*

trait ColumnOps { col1: Column =>
  import ColumnOps.*

  inline def +(col2: Column): Plus[col1.type, col2.type] = ${ plusImpl('col1, 'col2) }
  inline def -(col2: Column): Minus[col1.type, col2.type] = ${ minusImpl('col1, 'col2) }
  inline def *(col2: Column): Mult[col1.type, col2.type] = ${ multImpl('col1, 'col2) }
  inline def /(col2: Column): Div[col1.type, col2.type] = ${ divImpl('col1, 'col2) }
  inline def &&(col2: Column): And[col1.type, col2.type] = ${ andImpl('col1, 'col2) }
  inline def ||(col2: Column): Or[col1.type, col2.type] = ${ orImpl('col1, 'col2) }
  inline def ++(col2: Column): PlusPlus[col1.type, col2.type] = ${ plusPlusImpl('col1, 'col2) }
  inline def <(col2: Column): Lt[col1.type, col2.type] = ${ ltImpl('col1, 'col2) }
  inline def <=(col2: Column): Le[col1.type, col2.type] = ${ leImpl('col1, 'col2) }
  inline def >(col2: Column): Gt[col1.type, col2.type] = ${ gtImpl('col1, 'col2) }
  inline def >=(col2: Column): Ge[col1.type, col2.type] = ${ geImpl('col1, 'col2) }
  inline def ===(col2: Column): Eq[col1.type, col2.type] = ${ eqImpl('col1, 'col2) }
  inline def =!=(col2: Column): Ne[col1.type, col2.type] = ${ neImpl('col1, 'col2) }
}

object ColumnOps:
  type Plus[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (DoubleOptLike, DoubleOptLike) => CommonNumericType[T1, T2]
    case _ => Error
  ]

  def plusImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Plus[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[DoubleOptLike], Type.of[DoubleOptLike], "expected 2 numeric columns", "+", '{ _ + _ })
      .asExprOf[Plus[C1, C2]]

  type Minus[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (DoubleOptLike, DoubleOptLike) => CommonNumericType[T1, T2]
    case _ => Error
  ]

  def minusImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Minus[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[DoubleOptLike], Type.of[DoubleOptLike], "expected 2 numeric columns", "-", '{ _ - _ })
      .asExprOf[Minus[C1, C2]]

  type Mult[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (DoubleOptLike, DoubleOptLike) => CommonNumericType[T1, T2]
    case _ => Error
  ]

  def multImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Mult[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[DoubleOptLike], Type.of[DoubleOptLike], "expected 2 numeric columns", "*", '{ _ * _ })
      .asExprOf[Mult[C1, C2]]

  type Div[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (DoubleOptLike, DoubleOptLike) => DoubleOfCommonNullability[T1, T2]
    case _ => Error
  ]

  def divImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Div[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[DoubleOptLike], Type.of[DoubleOptLike], "expected 2 numeric columns", "/", '{ _ / _ })
      .asExprOf[Div[C1, C2]]

  type And[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (BooleanOptLike, BooleanOptLike) => CommonBooleanType[T1, T2]
    case _ => Error
  ]

  def andImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[And[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[BooleanOptLike], Type.of[BooleanOptLike], "expected 2 boolean columns", "&&", '{ _ && _ })
      .asExprOf[And[C1, C2]]

  type Or[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (BooleanOptLike, BooleanOptLike) => CommonBooleanType[T1, T2]
    case _ => Error
  ]

  def orImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Or[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[BooleanOptLike], Type.of[BooleanOptLike], "expected 2 boolean columns", "||", '{ _ || _ })
      .asExprOf[Or[C1, C2]]

  type PlusPlus[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (StringOptLike, StringOptLike) => StringOfCommonNullability[T1, T2]
    case _ => Error
  ]

  def plusPlusImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[PlusPlus[C1, C2]] =
    operatorImpl[C1, C2](col1, col2, Type.of[StringOptLike], Type.of[StringOptLike], "expected 2 string columns", "++", '{ (a, b) => concat(a, b) })
      .asExprOf[PlusPlus[C1, C2]]

  type Eq[C1 <: Column, C2 <: Column] = EqualityCheck[C1, C2]
  type Ne[C1 <: Column, C2 <: Column] = EqualityCheck[C1, C2]
  type Lt[C1 <: Column, C2 <: Column] = Compare[C1, C2]
  type Le[C1 <: Column, C2 <: Column] = Compare[C1, C2]
  type Gt[C1 <: Column, C2 <: Column] = Compare[C1, C2]
  type Ge[C1 <: Column, C2 <: Column] = Compare[C1, C2]

  def eqImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Eq[C1, C2]] =
    equalityCheckImpl(col1, col2, "===", '{ _ === _ }).asExprOf[Eq[C1, C2]]

  def neImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Ne[C1, C2]] =
    equalityCheckImpl(col1, col2, "=!=", '{ _ =!= _ }).asExprOf[Ne[C1, C2]]

  def ltImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Lt[C1, C2]] =
    comparisonImpl(col1, col2, "<", '{ _ < _ }).asExprOf[Lt[C1, C2]]

  def leImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Le[C1, C2]] =
    comparisonImpl(col1, col2, "<=", '{ _ <= _ }).asExprOf[Le[C1, C2]]

  def gtImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Gt[C1, C2]] =
    comparisonImpl(col1, col2, ">", '{ _ > _ }).asExprOf[Gt[C1, C2]]

  def geImpl[C1 <: Column : Type, C2 <: Column : Type](col1: Expr[C1], col2: Expr[C2])(using Quotes): Expr[Ge[C1, C2]] =
    comparisonImpl(col1, col2, ">=", '{ _ >= _ }).asExprOf[Ge[C1, C2]]

  ////////////////////////////////

  type Compare[C1 <: Column, C2 <: Column] = ColumnFun2[C1, C2, [T1, T2] =>> (T1, T2) match
    case (NonNullable, NonNullable) => BooleanNotNull
    case _ => BooleanOrNull
  ]

  type EqualityCheck[C1 <: Column, C2 <: Column] = Compare[C1, C2]

  def operatorImpl[C1 <: Column : Type, C2 <: Column : Type](
    col1: Expr[C1], col2: Expr[C2],
    expectedT1: Type[?], expectedT2: Type[?],
    failureExplanation: String,
    opName: String,
    op: Expr[(UntypedColumn, UntypedColumn) => UntypedColumn]
  )(using Quotes): Expr[ColumnImpl] =
    import quotes.reflect.*
    (Type.of[C1], Type.of[C2]) match {
      case ('[Col[t1]], '[Col[t2]]) =>
        val tt1 = TypeRepr.of[t1]
        val tt2 = TypeRepr.of[t2]
        val expectedTt1 = TypeRepr.of(using expectedT1)
        val expectedTt2 = TypeRepr.of(using expectedT2)
        if tt1 <:< expectedTt1 && tt2 <:< expectedTt2 then
          '{ ColumnImpl(${ op }(${col1}.untyped, ${col2}.untyped)) }
        else
          val msg = s"Cannot apply operator $opName to columns of types ${showDataTypeSimple(tt1)} and ${showDataTypeSimple(tt2)}: $failureExplanation"
          report.errorAndAbort(msg)
      case _ =>
        report.errorAndAbort(s"Cannot apply operator $opName to non-column arguments")
    }

  def equalityCheckImpl[C1 <: Column : Type, C2 <: Column : Type](
    col1: Expr[C1],
    col2: Expr[C2],
    opName: String,
    op: Expr[(UntypedColumn, UntypedColumn) => UntypedColumn]
  )(using Quotes): Expr[ColumnImpl] =
    import quotes.reflect.*
    (Type.of[C1], Type.of[C2]) match {
      case ('[Col[t1]], '[Col[t2]]) =>
        import org.virtuslab.iskra.types.TypesCanEqual
        Expr.summon[TypesCanEqual[t1, t2]] match {
          case Some(_) =>
            '{ ColumnImpl(${op}(${col1}.untyped, ${col2}.untyped)) }
          case None =>
            val failureExplanation = "Implementation of TypesCanEqual for these types is missing"
            val msg = s"Cannot apply operator $opName to columns of types ${showDataTypeSimple(TypeRepr.of[t1])} and ${showDataTypeSimple(TypeRepr.of[t2])}: $failureExplanation"
            report.errorAndAbort(msg)
        }
      case _ =>
        report.errorAndAbort(s"Cannot apply operator $opName to non-column arguments")
    }

  def comparisonImpl[C1 <: Column : Type, C2 <: Column : Type](
    col1: Expr[C1],
    col2: Expr[C2],
    opName: String,
    op: Expr[(UntypedColumn, UntypedColumn) => UntypedColumn]
  )(using Quotes): Expr[ColumnImpl] =
    import quotes.reflect.*
    (Type.of[C1], Type.of[C2]) match {
      case ('[Col[t1]], '[Col[t2]]) =>
        import org.virtuslab.iskra.types.TypesOrdering
        Expr.summon[TypesOrdering[t1, t2]] match {
          case Some(_) =>
            '{ ColumnImpl(${op}(${col1}.untyped, ${col2}.untyped)) }
          case None =>
            val failureExplanation = "Implementation of TypesOrdering for these types is missing"
            val msg = s"Cannot apply operator $opName to columns of types ${showDataTypeSimple(TypeRepr.of[t1])} and ${showDataTypeSimple(TypeRepr.of[t2])}: $failureExplanation"
            report.errorAndAbort(msg)
        }
      case _ =>
        report.errorAndAbort(s"Cannot apply operator $opName to non-column arguments")
    }

  private def showDataTypeSimple(using Quotes)(tpe: quotes.reflect.TypeRepr): String =
    import quotes.reflect.*
    tpe.show.replaceAll("""org\.virtuslab\.iskra\.types\.""", "")
