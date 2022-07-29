package org.virtuslab.example

import org.virtuslab.typedframes.api.*
import org.virtuslab.typedframes.functions.{lit, avg}

object HelloSpark {
  case class JustInt(i: Int)

  case class Foo(a: String, b: Int)
  case class Bar(b: Int, c: Option[String])
  case class Nif(c: String, d: Int)

  case class FooBar(a: String, b: Int, c: String)

  case class Baz(i: Int, str: String)

  case class PersonName(first: String, last: String)
  case class Person(id: Int, name: PersonName)
  case class FlatPerson(id: Int, firstName: String, lastName: String)

  case class XXX(x1: Int, x2: String)
  case class YYY(y1: Int, y2: String)

  def main(args: Array[String]): Unit = {
    given spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("Hello Spark")
        .getOrCreate()
    }

    val untypedInts = 
      import spark.implicits._
      Seq(1, 2, 3, 4).toDF("int")
      
    untypedInts.show()

    val typedInts = untypedInts.typed[JustInt]

    val ints = Seq(1, 2, 3, 4).toTypedDF
    ints.show()

    println(ints.collectAs[Int])

    val strings = Seq("abc", "def").toTypedDF
    strings.show()

    val foos = Seq(
      Foo("aaaa", 1),
      Foo("bbbb", 2)
    ).toTypedDF

    foos.show()

    println(foos.select($.b.as("i")).collectAs[JustInt])

    foos.select($.b.as("b1")).show()
    foos.select(($.b + $.b).as("b2")).show()
    foos.select($.b, $.b).show()

    val bazs = foos.select(($.b + $.b).as("i"), $.a.as("str"))

    bazs.show()

    println(bazs.collectAs[Baz])

    val persons = Seq(
      Person(1, PersonName("William", "Shakespeare"))
    ).toTypedDF

    persons.show()

    persons.select($.name).show()

    Seq(FlatPerson(1, "William", "Shakespeare"))
      .toTypedDF
      .select(($.firstName ++ lit(" ") ++ $.lastName).as("fullName"))
      .show()

    ///////

    // TODOs:

    // persons.select($.name.first).show()

    ////////

    val bars = Seq(
      Bar(1, Some("XXX")),
      Bar(2, None)
    ).toTypedDF

    foos.as("foos").join(bars.as("bars")).on($.foos.b === $.bars.b).select($.a, $.bars.b, $.c).show()
    foos.join(bars).on($.foos.b === $.bars.b).select($.a, $.bars.b, $.c).show()
    
    val fooos = foos.as("fooos")
    val baars = bars.as("baars")

    fooos.join(baars).on($.fooos.b === $.baars.b).select($.a, $.baars.b, $.c).show()

    val nifs = Seq(Nif("XXX", -1), Nif("YYY", -2)).toTypedDF

    foos
      .join(bars).on($.foos.b === $.bars.b)
      .join(nifs).on($.bars.c === $.nifs.c)
      .show()

    ////////

    val avgFoos = foos.groupBy($.a).agg(($.a ++ lit("!!!")).as("aaa"), (avg($.b) + lit(1.0d)).as("average"))
    avgFoos.show()

    avgFoos.select($.average, $.aaa).show()

    spark.stop()
  }
}
