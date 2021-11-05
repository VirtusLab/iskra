package org.virtuslab.typedspark

import scala.quoted.*

// Utils for debugging

inline def showType[A] = ${showTypeImpl[A]}

def showTypeImpl[A : Type](using quotes: Quotes): Expr[Unit] =
  import quotes.reflect.*
  println("*********")
  println(TypeRepr.of[A].show)
  println(TypeRepr.of[A].widen.show)
  '{()}


inline def getType[A] = ${getTypeImpl[A]}

def getTypeImpl[A : Type](using quotes: Quotes): Expr[String] =
  import quotes.reflect.*
  Expr(TypeRepr.of[A].show)