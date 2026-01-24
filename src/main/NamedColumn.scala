package org.virtuslab.iskra

import scala.language.implicitConversions

import scala.quoted.*
import types.DataType

trait NamedColumnOrColumnsLike

object NamedColumnOrColumnsLike:
  // TODO: Better error message when the conversion cannot be applied
  implicit transparent inline def columnToNamedColumn(inline col: Column): NamedColumn[?, ?] =
    ${ columnToNamedColumnImpl('col) }

  private def columnToNamedColumnImpl(col: Expr[Column])(using Quotes): Expr[NamedColumn[?, ?]] =
    import quotes.reflect.*
    col match
      case '{ ($v: StructuralSchemaView).selectDynamic($nm: Name).$asInstanceOf$[Col[tp]] } =>
        nm.asTerm.tpe.asType match
          case '[Name.Subtype[n]] =>
            '{ NamedColumn[n, tp](${ col }.untyped.as(${ nm })) }
      case '{ $c: Col[tp] } =>
        col.asTerm match
          case Inlined(_, _, Ident(name)) =>
            ConstantType(StringConstant(name)).asType match
              case '[Name.Subtype[n]] =>
                val alias = Literal(StringConstant(name)).asExprOf[Name]
                '{ NamedColumn[n, tp](${ col }.untyped.as(${ alias })) }


opaque type NamedColumns >: Repeated[NamedColumnOrColumnsLike] = Repeated[NamedColumnOrColumnsLike]

class NamedColumn[N <: Name, T <: DataType](val untyped: UntypedColumn)
  extends NamedColumnOrColumnsLike

class ColumnsWithSchema[Schema <: Tuple](val underlyingColumns: Seq[UntypedColumn]) extends NamedColumnOrColumnsLike


type ColumnLabel = Name | (Name / Name)

@annotation.showAsInfix
trait :=[L <: ColumnLabel, T <: DataType]

@annotation.showAsInfix
trait /[+Prefix <: Name, +Suffix <: Name]
