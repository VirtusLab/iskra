package org.virtuslab.typedframes.example.books

import org.virtuslab.typedframes.api.*

@main def runExample(dataFilePath: String): Unit =
  given spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("books")
      .getOrCreate()
  }

  case class Book(title: String, author: String, publicationYear: Int)

  val untypedBooks: UntypedDataFrame = spark.read.options(Map("header"->"true")).csv(dataFilePath) // UntypedDataFrame = sql.DataFrame
  untypedBooks.show()
  val books = untypedBooks.typed[Book] // Unsafe: make sure `untypedBooks` has the right schema

  import org.apache.spark.sql.functions.lower

  val authorlessBooks = books.select(
    lower($.title.untyped).typed[StringType].as("title"),
    $.publicationYear
  )
  authorlessBooks.show()



import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.shouldEqual
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files

class ExampleTest extends AnyFunSuite:
  test("Books example") {
    val fileContent = """title,author,publicationYear
                        |"The Call of Cthulhu","H. P. Lovecraft",1928
                        |"The Hobbit, or There and Back Again","J. R. R. Tolkien",1937
                        |"Alice's Adventures in Wonderland","Lewis Carroll",1865
                        |"Murder on the Orient Express","Agatha Christie",1934
    """.stripMargin

    val file = File.createTempFile("books", ".csv")
    file.deleteOnExit()

    Files.write(file.getAbsoluteFile.toPath, fileContent.getBytes)

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) { runExample(file.getAbsolutePath) }
    val result = new String(outCapture.toByteArray)

    val expected  = """+--------------------+----------------+---------------+
                      ||               title|          author|publicationYear|
                      |+--------------------+----------------+---------------+
                      || The Call of Cthulhu| H. P. Lovecraft|           1928|
                      ||The Hobbit, or Th...|J. R. R. Tolkien|           1937|
                      ||Alice's Adventure...|   Lewis Carroll|           1865|
                      ||Murder on the Ori...| Agatha Christie|           1934|
                      |+--------------------+----------------+---------------+
                      |
                      |+--------------------+---------------+
                      ||               title|publicationYear|
                      |+--------------------+---------------+
                      || the call of cthulhu|           1928|
                      ||the hobbit, or th...|           1937|
                      ||alice's adventure...|           1865|
                      ||murder on the ori...|           1934|
                      |+--------------------+---------------+
                      |
                      |""".stripMargin

    result shouldEqual expected
  }
