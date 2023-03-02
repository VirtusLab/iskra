package org.virtuslab.iskra

import scala.quoted.*
import scala.reflect.ClassTag

import types.{DataType, Encoder, StructEncoder}


class ClassDataFrame[A](val untyped: UntypedDataFrame) extends DataFrame

object ClassDataFrame:
  extension [A](df: ClassDataFrame[A])
    transparent inline def asStruct: StructDataFrame[?] = ${ asStructImpl('df) }

    inline def collect(): Array[A] = ${ collectImpl('df) }

  private def asStructImpl[A : Type](df: Expr[ClassDataFrame[A]])(using Quotes): Expr[StructDataFrame[?]] =
    import quotes.reflect.report
    
    Expr.summon[Encoder[A]] match
      case Some(encoder) => encoder match
        case '{ $enc: StructEncoder[A] { type StructSchema = structSchema } } =>
          '{ StructDataFrame[structSchema](${ df }.untyped) }    
        case '{ $enc: Encoder[A] { type ColumnType = colType } } =>
          Type.of[colType] match
            case '[DataType.Subtype[t]] =>
              '{ StructDataFrame[("value" := t)/*  *: EmptyTuple */](${ df }.untyped) } // TODO: Get rid of non-tuple schemas? 
      case None => report.errorAndAbort(s"Could not summon encoder for ${Type.show[A]}") 

  private def collectImpl[A : Type](df: Expr[ClassDataFrame[A]])(using Quotes): Expr[Array[A]] =
    import quotes.reflect.report
    
    Expr.summon[Encoder[A]] match
      case Some(encoder) =>
        val classTag = Expr.summon[ClassTag[A]].getOrElse(report.errorAndAbort(s"Could not summon ClassTag for ${Type.show[A]}"))
        encoder match
          case '{ $enc: StructEncoder[A] { type StructSchema = structSchema } } =>
            '{ ${ df }.untyped.collect.map(row => ${ enc }.decode(row).asInstanceOf[A])(${ classTag }) }    
          case '{ $enc: Encoder[A] { type ColumnType = colType } } =>
            '{ ${ df }.untyped.collect.map(row => ${ enc }.decode(row(0)).asInstanceOf[A])(${ classTag }) }
      case None => report.errorAndAbort(s"Could not summon encoder for ${Type.show[A]}")
