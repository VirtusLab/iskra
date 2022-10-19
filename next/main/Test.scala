package iskra.test.cross

// import org.virtuslab.iskra._

// case class Foo(i: Int)

// import org.virtuslab.iskra.SeqTypingOps

// import org.virtuslab.iskra.types.{Encoder, StructEncoder}
// val z = Encoder.fromMirror[Foo]
// val x = summon[StructEncoder[Foo]]
// val y = summon[Encoder[Foo]]

// given spark: SparkSession = ???
// // val elements = Seq(1, 2, 3).toTypedDF.collectAs[Int]
// val elements = toSeqTypingOps(Seq(Foo(1), Foo(2), Foo(3))).toTypedDF.collectAs[Foo]

import org.virtuslab.iskra._
import org.virtuslab.iskra.lambda._
case class Foo(string: String)

object Test {

  def main(args: Array[String]) = { 
    implicit lazy val spark: SparkSession = 
      SparkSession
        .builder()
        .master("local")
        .appName("test")
        .getOrCreate()

    val df = Seq(Foo("abc")).toTypedDF
    // df.select($ => $.string)
    // val elements = df.select($.string)

    // elements.show()
  }

}