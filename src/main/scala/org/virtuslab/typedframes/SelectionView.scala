package org.virtuslab.typedframes

import scala.quoted._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame => UntypedDataFrame}
import types.{DataType, StructType}

trait SelectionView extends Selectable:
  def frameAliases: Seq[String] // TODO: get rid of this in runtime
  
  // TODO: What should be the semantics of `*`? How to handle ambiguous columns?
  // type AllColumns <: Tuple
  // def * : AllColumns
  
  def selectDynamic(name: String): AliasedSelectionView | LabeledColumn[?, ?] =
    if frameAliases.contains(name)
      then AliasedSelectionView(name)
      else LabeledColumn(col(Name.escape(name)))

object SelectionView:
  type Subtype[T <: SelectionView] = T

  // private def reifyColumns[T <: Tuple : Type](using Quotes): Expr[Tuple] = reifyCols(Type.of[T])

  // private def reifyCols(using Quotes)(schemaType: Type[?]): Expr[Tuple] =
  //   import quotes.reflect.*
  //   schemaType match
  //     case '[EmptyTuple] => '{ EmptyTuple }
  //     case '[LabeledColumn[headLabel1, headType] *: tail] =>
  //       headLabel1 match
  //         case '[Name.Subtype[name]] => // TODO: handle frame prefixes
  //           val label = Expr(Type.valueOfConstant[name].get.toString)
  //           '{ TypedColumn[Nothing](col(Name.escape(${ label }))) *: ${ reifyCols(Type.of[tail]) } }

  private def refineType(using Quotes)(base: quotes.reflect.TypeRepr, refinements: List[(String, quotes.reflect.TypeRepr)]): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    refinements match
      case Nil => base
      case (name, info) :: refinementsTail =>
        val newBase = Refinement(base, name, info)
        refineType(newBase, refinementsTail)

  private def selectionViewType(using Quotes)(base: quotes.reflect.TypeRepr, schemaType: Type[?]): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    schemaType match
      case '[EmptyTuple] => base
      case '[LabeledColumn[headLabel, headType] *: tail] => // TODO: get rid of duplicates
        val nameType = Type.of[headLabel] match
          case '[Name.Subtype[name]] => Type.of[name]
          case '[(Name.Subtype[framePrefix], Name.Subtype[name])] => Type.of[name]
        val name = nameType match
          case '[n] => Type.valueOfConstant[n].get.toString
        val info = TypeRepr.of[LabeledColumn[headLabel, headType]]
        val newBase = Refinement(base, name, info)
        selectionViewType(newBase, Type.of[tail])

  def selectionViewExpr[DF <: DataFrame[?] : Type](using Quotes): Expr[SelectionView] =
    import quotes.reflect.*
    Type.of[DF] match
      case '[DataFrame[schema]] =>
        val schemaType = Type.of[schema]
        val aliasViewsByName = frameAliasViewsByName(schemaType)
        val columns = unambiguousColumns(schemaType)    
        val frameAliasNames = Expr(aliasViewsByName.map(_._1))
        val baseType = TypeRepr.of[SelectionView /* { type AllColumns = schema } */]
        val viewType = refineType(refineType(baseType, columns), aliasViewsByName) // TODO: conflicting name of frame alias and column?

        viewType.asType match
          case '[SelectionView.Subtype[t]] => '{
            new SelectionView {
              override def frameAliases: Seq[String] = ${ frameAliasNames }
              // type AllColumns = schema 
              // override def * : AllColumns =
              //   // TODO FIX:
              //   // ${ reifyColumns[schema] }.asInstanceOf[AllColumns]
            }.asInstanceOf[t]
          }

  def allPrefixedColumns(using Quotes)(schemaType: Type[?]): List[(String, (String, quotes.reflect.TypeRepr))] =
    import quotes.reflect.*
    schemaType match
      case '[EmptyTuple] => List.empty
      case '[LabeledColumn[Name.Subtype[name], dataType] *: tail] =>
        allPrefixedColumns(Type.of[tail])
      case '[LabeledColumn[(Name.Subtype[framePrefix], Name.Subtype[name]), dataType] *: tail] =>
        val prefix = Type.valueOfConstant[framePrefix].get.toString
        val colName = Type.valueOfConstant[name].get.toString
        (prefix -> (colName -> TypeRepr.of[LabeledColumn[name, dataType]])) :: allPrefixedColumns(Type.of[tail])

  def frameAliasViewsByName(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr /* Seq[quotes.reflect.TypeRepr] */)] =
    import quotes.reflect.*
    allPrefixedColumns(schemaType).groupBy(_._1).map { (frameName, values) =>
      val columnTypes = values.map(_._2)
      frameName -> refineType(TypeRepr.of[AliasedSelectionView], columnTypes)
    }.toList

  def unambiguousColumns(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr)] =
    allColumns(schemaType).groupBy(_._1).collect {
      case (name, List((_, col))) => name -> col
    }.toList

  def allColumns(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr)] =
    import quotes.reflect.*
    schemaType match
      case '[EmptyTuple] => List.empty
      case '[LabeledColumn[Name.Subtype[name], dataType] *: tail] =>
        val colName = Type.valueOfConstant[name].get.toString
        val namedColumn = colName -> TypeRepr.of[LabeledColumn[name, dataType]]
        namedColumn :: allColumns(Type.of[tail])
      case '[LabeledColumn[(Name.Subtype[framePrefix], Name.Subtype[name]), dataType] *: tail] =>
        val colName = Type.valueOfConstant[name].get.toString
        val namedColumn = colName -> TypeRepr.of[LabeledColumn[name, dataType]]
        namedColumn :: allColumns(Type.of[tail])
class AliasedSelectionView(frameAliasName: String) extends Selectable:
  def selectDynamic(name: String): LabeledColumn[Name, DataType] =
    val columnName = s"${Name.escape(frameAliasName)}.${Name.escape(name)}"
    LabeledColumn[Name, DataType](col(columnName))