// import annotation.showAsInfix

// type Name = String & Singleton
// type Label = Name | (Name, Name)

// trait DataType

// trait IntegerType extends DataType
// trait StringType extends DataType
// trait BooleanType extends DataType

// @showAsInfix
// trait %>[Label, DataType] 

// sealed trait FrameSchema
// object SNil extends FrameSchema
// type SNil = SNil.type
// @showAsInfix
// class &:[H <: %>[?, ?], T <: FrameSchema] extends FrameSchema

// type MySchema = "a" %> StringType &: "b" %> IntegerType &: SNil
// // type MySchema = ("a" &> StringType) %: ("b" &> IntegerType) %: SNil


// // type Name = String & Singleton

// // trait Select[Aliases <: Tuple, N <: Name]:
// //   type Selected
// //   def selected(aliases: Aliases): Selected
// // object Select:
// //   // transparent inline given select[Aliases <: Tuple, N <: Name]: Select[Aliases, N] = selectImpl
// //   // given select1[A, AliasesTail <: Tuple, N <: Name]: Select[(N, A) *: AliasesTail, N] = new Select[(N, A) *: AliasesTail, N]:
// //   given select1[A, AliasesTail <: Tuple, N <: Name]: Select[(N, A) *: AliasesTail, N] with
// //     type Selected = A
// //     def selected(aliases: (N, A) *: AliasesTail): Selected = aliases.head._2

// //   given select2[A, AliasesTail <: Tuple, N <: Name](using s: Select[AliasesTail, N]): Select[(N, A) *: AliasesTail, N] with
// //     type Selected = s.Selected
// //     def selected(aliases: (N, A) *: AliasesTail): Selected = s.selected(aliases.tail)

// // trait DF { outerDF =>
// //   type Aliases <: Tuple
// //   def aliases: Aliases

// //   def withAlias[N <: Name, D <: DF & Singleton](name: N, df: D) =
// //     new DF:
// //       type Aliases = ((N, df.type)) *: outerDF.Aliases
// //       def aliases: Aliases = ((name, df)) *: outerDF.aliases

// //   def select[N <: Name](name: N)(using s: Select[Aliases, N]): s.Selected = s.selected(aliases)
// // }

// // object DF:
// //   def empty = new DF:
// //     def aliases = EmptyTuple
// //     type Aliases = EmptyTuple


// // // trait Ctx:
// // //   type Aliases <: Tuple

// // // def ref(using ctx: Ctx)(name: String)(using aliasRef: AliasRef[name.type]): aliasRef.Ref = aliasRef.ref

// // // trait AliasRef[Alias <: Name]:
// // //   type Ref <: DF
// // //   def ref: Ref


// // @main def run() =
// //   val df1 = DF.empty
// //   val df2 = DF.empty
// //   val df3 = DF.empty.withAlias("df1", df1)//.withAlias("df2", df2)
// //   val df4 = DF.empty.withAlias("df1", df1).withAlias("df3", df3)
  
// //   // val sel = Select.select1[df1.type, EmptyTuple, "df1"]
  
// //   val y: df1.type = df3.select("df1")
// //   println(y)

// //   // given Ctx with {}
// //   // given AliasRef["foo"] with
// //   //   type Ref = df1.type
// //   //   def ref = df1
// //   // val x = ref("foo")
// //   // println(x)
    