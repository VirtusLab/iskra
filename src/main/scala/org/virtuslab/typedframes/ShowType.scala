package org.virtuslab.typedframes

import scala.quoted.*

// Utils for debugging

inline def showType[A] = ${showTypeImpl[A]}

def showTypeImpl[A : Type](using quotes: Quotes): Expr[Unit] =
  import quotes.reflect.*
  println("*********")
  println(TypeRepr.of[A].show)
  println(TypeRepr.of[A].widen.show)
  println(TypeRepr.of[A].dealias.show)
  '{()}


inline def getTypeName[A] = ${getTypeNameImpl[A]}

def getTypeNameImpl[A : Type](using quotes: Quotes): Expr[String] =
  import quotes.reflect.*
  Expr(TypeRepr.of[A].show)