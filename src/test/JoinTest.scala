package org.virtuslab.iskra.test

class JoinTest extends SparkUnitTest:
  import org.virtuslab.iskra.api.*
  import functions.lit
  import Column.=== // by default shadowed by === from scalatest
  

  case class Foo(int: Int, long: Long)
  case class Bar(int: Int, string: String)

  val foos = Seq(
    Foo(1, 10),
    Foo(2, 20)
  ).toDF.asStruct

  val bars = Seq(
    Bar(2, "b"),
    Bar(3, "c")
  ).toDF.asStruct

  test("join-inner-on") {
    val joined = foos.join(bars).on($.foos.int === $.bars.int)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerType,
      "foos" / "long" := LongType,
      "bars" / "int" := IntegerType,
      "bars" / "string" := StringType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
      $.bars.int.as("_3"),
      $.bars.string.as("_4")
    ).asClass[(Int, Long, Int, String)].collect().toList

    result shouldEqual List(
      (2, 20, 2, "b")
    )
  }

  test("join-left-on") {
    val joined = foos.leftJoin(bars).on($.foos.int === $.bars.int)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerType,
      "foos" / "long" := LongType,
      "bars" / "int" := IntegerOptType,
      "bars" / "string" := StringOptType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
      $.bars.int.as("_3"),
      $.bars.string.as("_4")
    ).asClass[(Int, Long, Option[Int], Option[String])].collect().toList

    result shouldEqual List(
      (1, 10, None, None),
      (2, 20, Some(2), Some("b"))
    )
  }

  test("join-right-on") {
    val joined = foos.rightJoin(bars).on($.foos.int === $.bars.int)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerOptType,
      "foos" / "long" := LongOptType,
      "bars" / "int" := IntegerType,
      "bars" / "string" := StringType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
      $.bars.int.as("_3"),
      $.bars.string.as("_4")
    ).asClass[(Option[Int], Option[Long], Int, String)].collect().toList

    result shouldEqual List(
      (Some(2), Some(20), 2, "b"),
      (None, None, 3, "c")
    )
  }

  test("join-full-on") {
    val joined = foos.fullJoin(bars).on($.foos.int === $.bars.int)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerOptType,
      "foos" / "long" := LongOptType,
      "bars" / "int" := IntegerOptType,
      "bars" / "string" := StringOptType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
      $.bars.int.as("_3"),
      $.bars.string.as("_4")
    ).asClass[(Option[Int], Option[Long], Option[Int], Option[String])].collect().toList

    result shouldEqual List(
      (Some(1), Some(10), None, None),
      (Some(2), Some(20), Some(2), Some("b")),
      (None, None, Some(3), Some("c"))
    )
  }

  test("join-semi-on") {
    val joined = foos.semiJoin(bars).on($.foos.int === $.bars.int)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerType,
      "foos" / "long" := LongType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
    ).asClass[(Int, Long)].collect().toList

    result shouldEqual List(
      (2, 20)
    )
  }

  test("join-anti-on") {
    val joined = foos.antiJoin(bars).on($.foos.int === $.bars.int)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerType,
      "foos" / "long" := LongType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
    ).asClass[(Int, Long)].collect().toList

    result shouldEqual List(
      (1, 10)
    )
  }

  test("join-cross") {
    val joined = foos.crossJoin(bars)

    val typedJoined: StructDataFrame[(
      "foos" / "int" := IntegerType,
      "foos" / "long" := LongType,
      "bars" / "int" := IntegerType,
      "bars" / "string" := StringType
    )] = joined

    val result = joined.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
      $.bars.int.as("_3"),
      $.bars.string.as("_4")
    ).asClass[(Int, Long, Int, String)].collect().toList

    result shouldEqual List(
      (1, 10, 2, "b"),
      (1, 10, 3, "c"),
      (2, 20, 2, "b"),
      (2, 20, 3, "c"),
    )
  }

  test("join-preserve-aliases") {
    val joined1 = foos.as("fooz").join(bars).on($.fooz.int === $.bars.int)
    val barz = bars.as("barz")
    val joined2 = foos.join(barz).on($.foos.int === $.barz.int)

    val result1 = joined1.select(
      $.fooz.int.as("_1"),
      $.fooz.long.as("_2"),
      $.bars.int.as("_3"),
      $.bars.string.as("_4")
    ).asClass[(Int, Long, Int, String)].collect().toList

    val result2 = joined2.select(
      $.foos.int.as("_1"),
      $.foos.long.as("_2"),
      $.barz.int.as("_3"),
      $.barz.string.as("_4")
    ).asClass[(Int, Long, Int, String)].collect().toList

    result1 shouldEqual List(
      (2, 20, 2, "b")
    )

    result2 shouldEqual List(
      (2, 20, 2, "b")
    )
  }
