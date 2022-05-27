//> using scala "3.2.0-RC0-bin-SNAPSHOT"
//> using lib org.apache.spark:spark-core_2.13:3.2.0
//> using lib org.apache.spark:spark-sql_2.13:3.2.0
//> using lib io.github.vincenzobaz::spark-scala3:0.1.3

package org.virtuslab.example

import scala3encoders.given

import org.apache.spark.sql.SparkSession
import org.virtuslab.typedframes.types.StructType.SCons
import org.virtuslab.typedframes.types.StringType

// case class JustInt(int: Int) // int - forbidden field name in a case class? - check at compiletime
case class JustInt(i: Int)

case class Quux(abc: String)

case class Foo(a: String, b: Int, bb: Int)
case class Bar(b: Int, bb: Int, c: String)

case class FooBar(a: String, b: Int, c: String)

case class Baz1(i: Int, str: String)
case class Baz2(str: String, i: Int)

case class Name(first: String, last: String)
case class Person(id: Int, name: Name)

object HellSpark {
  def main(args: Array[String]): Unit = {
    implicit lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }

    import spark.implicits._

    import org.virtuslab.typedframes.{*, given}
    
    val untypedInts = Seq(1, 2, 3, 4).toDF("int")
    untypedInts.show()

    val typedInts = untypedInts.typed[JustInt]

    val ints = Seq(1, 2, 3, 4).toTypedDF("i")
    ints.show()

    val strings = Seq("abc", "def").toTypedDF("ab")
    strings.show()

    strings.select($.ab, $.ab.named("abcde")).select($.abcde).show()

    import types.{DataType, StructType}

    val foos = Seq(
      Foo("aaaa", 1, 10),
      Foo("bbbb", 2, 20)
    ).toTypedDF

    foos.show()

    foos.select($.b.named("b1")).show()
    foos.select(($.b + $.b).named["b1"]).show()
    foos.select(($.b + $.b)).show()
    foos.select($.b, $.b).show()

    val afterSelect = foos.select(($.b + $.b).named("i"), $.a.named("str"))

    afterSelect.show()

    println(afterSelect.collect[Baz1]())

    // // afterSelect.select($.bc.named["bbb"]).show() // <- This won't compile


    val persons = Seq(
      Person(1, Name("William", "Shakespeare"))
    ).toTypedDF

    persons.select($.name).show()

    // TODOs:

    // persons.select($.name.first).show()

    spark.stop()
  }
}
