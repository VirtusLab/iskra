package org.virtuslab.iskra.example.workers

import org.virtuslab.iskra.api.*
import functions.lit

@main def runExample(): Unit =
  given spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("workers")
      .getOrCreate()
  }
  
  case class Worker(id: Long, firstName: String, lastName: String, yearsInCompany: Int)
  case class Supervision(subordinateId: Long, supervisorId: Long)
  case class Room(number: Int, name: String, desksCount: Int)

  val workers = Seq(
    Worker(3, "Bob", "Smith", 8),
    Worker(13, "Alice", "Potter", 4),
    Worker(38, "John", "Parker", 1),
    Worker(21, "Julia", "Taylor", 3),
    Worker(11, "Emma", "Brown", 6),
    Worker(8, "Michael", "Johnson", 7),
    Worker(18, "Natalie", "Evans", 4),
    Worker(22, "Paul", "Wilson", 3),
    Worker(44, "Daniel", "Jones", 1)
  ).toDF.asStruct

  val supervisions = Seq(
    44 -> 21, 22 -> 21, 38 -> 13, 11 -> 3, 21 -> 18, 13 -> 8, 3 -> 8, 18 -> 8
  ).map{ case (id1, id2) => Supervision(id1, id2) }.toDF.asStruct

  workers.as("subordinates")
    .leftJoin(supervisions).on($.subordinates.id === $.subordinateId)
    .leftJoin(workers.as("supervisors")).on($.supervisorId === $.supervisors.id)
    .select {
      val salary = (lit(4732) + $.subordinates.yearsInCompany * lit(214)).as("salary")
      val supervisor = ($.supervisors.firstName ++ lit(" ") ++ $.supervisors.lastName).as("supervisor")
      ($.subordinates.firstName, $.subordinates.lastName, supervisor, salary)
    }
    .where($.salary > lit(5000))
    .show()

  spark.stop()



import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.shouldEqual
import java.io.ByteArrayOutputStream

class ExampleTest extends AnyFunSuite:
  test("Workers example") {
    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) { runExample() }
    val result = new String(outCapture.toByteArray)

    val expected = """|+---------+--------+---------------+------+
                      ||firstName|lastName|     supervisor|salary|
                      |+---------+--------+---------------+------+
                      ||  Michael| Johnson|           null|  6230|
                      ||     Emma|   Brown|      Bob Smith|  6016|
                      ||      Bob|   Smith|Michael Johnson|  6444|
                      ||    Alice|  Potter|Michael Johnson|  5588|
                      ||  Natalie|   Evans|Michael Johnson|  5588|
                      ||    Julia|  Taylor|  Natalie Evans|  5374|
                      ||     Paul|  Wilson|   Julia Taylor|  5374|
                      |+---------+--------+---------------+------+
                      |
                      |""".stripMargin

    result shouldEqual expected
  }
