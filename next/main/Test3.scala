//> using target.scala "3"

package iskra.test.scala3

import org.virtuslab.iskra.*
import org.virtuslab.iskra.contextual.*
case class Foo(string: String, int: Int)

object Test {

  def main(args: Array[String]) = { 
    implicit lazy val spark: SparkSession = 
      SparkSession
        .builder()
        .master("local")
        .appName("test")
        .getOrCreate()

    val df = Seq(Foo("abc", 1)).toTypedDF
    df.select($.string).show()
    df.select(($.int + $.int).as("foo")).show()
  }

}