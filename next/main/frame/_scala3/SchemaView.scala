//> using target.scala "3"

package org.virtuslab.iskra
package frame

import scala.quoted._
import org.apache.spark.sql.functions.col
import types.DataType
import MacroHelpers.AsTuple

trait SchemaView extends Selectable:
  // protected def wrapColumn(col: UntypedColumn): TypedColumn[?]

  def universalColumns: Seq[String]
  def frameAliases: Seq[String] // TODO: get rid of this at runtime
  
  // def selectDynamic(name: String): AliasedSchemaView | LabeledColumn[?, ?] =
  def selectDynamic(name: String): Column =
    // if frameAliases.contains(name)
    //   then AliasedSchemaView(name)
    //   else LabeledColumn(col(Name.escape(name)))

    val untypedCol = col(Name.escape(name))
    if universalColumns.contains(name)
      then UniversalCol(untypedCol)
      else DataCol(untypedCol)

    // wrapColumn(col(Name.escape(name)))
    

object SchemaView:
  type Subtype[T <: SchemaView] = T

  private def refineType(using Quotes)(base: quotes.reflect.TypeRepr, refinements: List[(String, quotes.reflect.TypeRepr)]): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    refinements match
      case Nil => base
      case (name, info) :: refinementsTail =>
        val newBase = Refinement(base, name, info)
        refineType(newBase, refinementsTail)

  // private def schemaViewType(using Quotes)(base: quotes.reflect.TypeRepr, schemaType: Type[?]): quotes.reflect.TypeRepr =
  //   import quotes.reflect.*
  //   schemaType match
  //     case '[EmptyTuple] => base
  //     case '[LabeledColumn[headLabel, headType] *: tail] => // TODO: get rid of duplicates
  //       val nameType = Type.of[headLabel] match
  //         case '[Name.Subtype[name]] => Type.of[name]
  //         case '[(Name.Subtype[framePrefix], Name.Subtype[name])] => Type.of[name]
  //       val name = nameType match
  //         case '[n] => Type.valueOfConstant[n].get.toString
  //       val info = TypeRepr.of[LabeledColumn[headLabel, headType]]
  //       val newBase = Refinement(base, name, info)
  //       schemaViewType(newBase, Type.of[tail])

  def schemaViewExpr[Schema : Type](using Quotes): Expr[SchemaView] =
    import quotes.reflect.*
    val schemaType = Type.of[Schema]
    // val schemaType = Type.of[AsTuple[Schema]]

    val aliasViewsByName = List.empty[(String, TypeRepr)] // TODO: Mock
    // val aliasViewsByName = frameAliasViewsByName(schemaType)
    val frameAliasNames = Expr(aliasViewsByName.map(_._1))
    val baseType = TypeRepr.of[SchemaView]
    val columns = unambiguousColumns(schemaType)    
    val viewType = refineType(refineType(baseType, columns), aliasViewsByName) // TODO: conflicting name of frame alias and column?

    viewType.asType match
      case '[SchemaView.Subtype[t]] => '{
        new SchemaView {
          override def universalColumns: Seq[String] = Seq.empty // TODO
          override def frameAliases: Seq[String] = ${ frameAliasNames }
        }.asInstanceOf[t]
      }

  // def allPrefixedColumns(using Quotes)(schemaType: Type[?]): List[(String, (String, quotes.reflect.TypeRepr))] =
  //   import quotes.reflect.*
  //   schemaType match
  //     case '[EmptyTuple] => List.empty
  //     case '[(Name.Subtype[name] := dataType) *: tail] =>
  //       allPrefixedColumns(Type.of[tail])
  //     case '[(framePrefix / name := dataType) *: tail] =>
  //       val prefix = Type.valueOfConstant[framePrefix].get.toString
  //       val colName = Type.valueOfConstant[name].get.toString
  //       (prefix -> (colName -> TypeRepr.of[name := dataType])) :: allPrefixedColumns(Type.of[tail])

  // def frameAliasViewsByName(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr)] =
  //   import quotes.reflect.*
  //   allPrefixedColumns(schemaType).groupBy(_._1).map { (frameName, values) =>
  //     val columnTypes = values.map(_._2)
  //     frameName -> refineType(TypeRepr.of[AliasedSchemaView], columnTypes)
  //   }.toList

  def unambiguousColumns(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr)] =
    allColumns(schemaType).groupBy(_._1).collect {
      case (name, List((_, col))) => name -> col
    }.toList

  // TODO: Support UniversalCol's
  def allColumns(using Quotes)(schemaType: Type[?]): List[(String, quotes.reflect.TypeRepr)] =
    import quotes.reflect.*

    // def columnName(label: Type[?]) = label match
    //   case '[Name.Subtype[name]] => Type.of[name]
    //   case '[Name.Subtype[framePrefix] / Name.Subtype[name]] => Type.of[name]

    def namedCol[N <: Name : Type, T <: DataType : Type] =
      val colName = Type.valueOfConstant[N].get.toString
      colName -> TypeRepr.of[DataCol[T] As N]

    schemaType match
      case '[StructSchema.Empty] =>
        List.empty
      case '[Name.Subtype[name] := dataType] =>
        List(namedCol[name, dataType])
      case '[Name.Subtype[framePrefix] / Name.Subtype[name] := dataType] =>
        List(namedCol[name, dataType])
      case '[init ** (Name.Subtype[name] := dataType)] =>
        allColumns(Type.of[init]) :+ namedCol[name, dataType]
      case '[init ** (Name.Subtype[framePrefix] / Name.Subtype[name] := dataType)] =>
        allColumns(Type.of[init]) :+ namedCol[name, dataType]

      //   val colName = columnName(Type.of[label])
      //   val namedColumn = colName -> TypeRepr.of[dataType As name]
      //   allColumns(Type.of[init]) :+ namedColumn
      // case '[init ** (label := dataType)] =>
      //   val colName = columnName(Type.of[label])
      //   val namedColumn = colName -> TypeRepr.of[dataType As name]
      //   allColumns(Type.of[init]) :+ namedColumn

      // case '[EmptyTuple] => List.empty
      // case '[LabeledColumn[Name.Subtype[name], dataType] *: tail] =>
      //   val colName = Type.valueOfConstant[name].get.toString
      //   val namedColumn = colName -> TypeRepr.of[LabeledColumn[name, dataType]]
      //   namedColumn :: allColumns(Type.of[tail])
      // case '[LabeledColumn[Name.Subtype[framePrefix] / Name.Subtype[name], dataType] *: tail] =>
      //   val colName = Type.valueOfConstant[name].get.toString
      //   val namedColumn = colName -> TypeRepr.of[LabeledColumn[name, dataType]]
      //   namedColumn :: allColumns(Type.of[tail])



// class AliasedSchemaView(frameAliasName: String) extends Selectable:
//   def selectDynamic(name: String): LabeledColumn[Name, DataType] =
//     val columnName = s"${Name.escape(frameAliasName)}.${Name.escape(name)}"
//     //Name := DataType
//     LabeledColumn[Name, DataType](col(columnName))