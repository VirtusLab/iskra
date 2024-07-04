package org.virtuslab.iskra

import scala.quoted.*
import org.apache.spark.sql.functions.col
import types.DataType
import MacroHelpers.AsTuple

inline def $(using view: SchemaView): view.type = view

trait SchemaView

object SchemaView:
  type Subtype[T <: SchemaView] = T

  private[iskra] def exprForDataFrame[DF <: DataFrame : Type](using quotes: Quotes): Option[Expr[SchemaView]] =
    Type.of[DF] match
      case '[ClassDataFrame[a]] =>
        Expr.summon[SchemaViewProvider[a]].map {
          case '{ $provider } => '{ ${ provider }.view }
        }
      case '[StructDataFrame.Subtype[df]] =>
        Some(StructSchemaView.schemaViewExpr[df])

trait StructSchemaView extends SchemaView, Selectable:
  def frameAliases: Seq[String] // TODO: get rid of this at runtime
  
  // TODO: What should be the semantics of `*`? How to handle ambiguous columns?
  // type AllColumns <: Tuple
  // def * : AllColumns
  
  def selectDynamic(name: String): AliasedSchemaView | LabeledColumn[?, ?] =
    if frameAliases.contains(name)
      then AliasedSchemaView(name)
      else LabeledColumn(col(Name.escape(name)))

object StructSchemaView:
  type Subtype[T <: StructSchemaView] = T

  private def refineType(using Quotes)(base: quotes.reflect.TypeRepr, refinements: List[(String, quotes.reflect.TypeRepr)]): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    refinements match
      case Nil => base
      case (name, info) :: refinementsTail =>
        val newBase = Refinement(base, name, info)
        refineType(newBase, refinementsTail)

  private def schemaViewType(using Quotes)(base: quotes.reflect.TypeRepr, schemaType: Type[?]): quotes.reflect.TypeRepr =
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
        schemaViewType(newBase, Type.of[tail])

  // private def reifyColumns[T <: Tuple : Type](using Quotes): Expr[Tuple] = reifyCols(Type.of[T])

  // private def reifyCols(using Quotes)(schemaType: Type[?]): Expr[Tuple] =
  //   import quotes.reflect.*
  //   schemaType match
  //     case '[EmptyTuple] => '{ EmptyTuple }
  //     case '[LabeledColumn[headLabel1, headType] *: tail] =>
  //       headLabel1 match
  //         case '[Name.Subtype[name]] => // TODO: handle frame prefixes
  //           val label = Expr(Type.valueOfConstant[name].get.toString)
  //           '{ Col[Nothing](col(Name.escape(${ label }))) *: ${ reifyCols(Type.of[tail]) } }

  def schemaViewExpr[DF <: StructDataFrame[?] : Type](using Quotes): Expr[StructSchemaView] =
    import quotes.reflect.*
    Type.of[DF] match
      case '[StructDataFrame[schema]] =>
        val schemaType = Type.of[AsTuple[schema]]
        val aliasViewsByName = frameAliasViewsByName(schemaType)
        val columns = unambiguousColumns(schemaType)    
        val frameAliasNames = Expr(aliasViewsByName.map(_._1))
        val baseType = TypeRepr.of[StructSchemaView]
        val viewType = refineType(refineType(baseType, columns), aliasViewsByName) // TODO: conflicting name of frame alias and column?

        viewType.asType match
          case '[StructSchemaView.Subtype[t]] => 
            '{
              new StructSchemaView {
                override def frameAliases: Seq[String] = ${ frameAliasNames }
                // TODO: Reintroduce `*` selector
                // type AllColumns = schema 
                // override def * : AllColumns = ${ reifyColumns[schema] }.asInstanceOf[AllColumns]
              }.asInstanceOf[t]
            }

  def allPrefixedColumns(using Quotes)(schemaType: Type[?]): List[(String, (String, quotes.reflect.TypeRepr))] =
    import quotes.reflect.*
    schemaType match
      case '[EmptyTuple] => List.empty
      case '[(Name.Subtype[name] := dataType) *: tail] =>
        allPrefixedColumns(Type.of[tail])
      case '[(framePrefix / name := dataType) *: tail] =>
        val prefix = Type.valueOfConstant[framePrefix].get.toString
        val colName = Type.valueOfConstant[name].get.toString
        (prefix -> (colName -> TypeRepr.of[name := dataType])) :: allPrefixedColumns(Type.of[tail])
      // TODO: Handle Nothing as schemaType (which might appear as propagation of earlier errors)

  def frameAliasViewsByName(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr)] =
    import quotes.reflect.*
    allPrefixedColumns(schemaType).groupBy(_._1).map { (frameName, values) =>
      val columnsTypes = values.map(_._2)
      frameName -> refineType(TypeRepr.of[AliasedSchemaView], columnsTypes)
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
      case '[LabeledColumn[Name.Subtype[framePrefix] / Name.Subtype[name], dataType] *: tail] =>
        val colName = Type.valueOfConstant[name].get.toString
        val namedColumn = colName -> TypeRepr.of[LabeledColumn[name, dataType]]
        namedColumn :: allColumns(Type.of[tail])

class AliasedSchemaView(frameAliasName: String) extends Selectable:
  def selectDynamic(name: String): LabeledColumn[Name, DataType] =
    val columnName = s"${Name.escape(frameAliasName)}.${Name.escape(name)}"
    LabeledColumn[Name, DataType](col(columnName))