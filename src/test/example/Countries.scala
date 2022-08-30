package org.virtuslab.iskra.example.countries

import org.virtuslab.iskra.api.*
import functions.avg

@main def runExample(): Unit =
  given spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("countries")
      .getOrCreate()
  }
  
  case class City(name: String, population: Int)
  case class Country(name: String, continent: String, capital: String, population: Int, gdp: Int)

  val cities = Seq(
    City("Warsaw", 1794532),
    City("Krakow", 769595),
    City("Paris", 11142303),
    City("Washington", 718355),
    City("London", 	9540576),
    City("Ottawa", 	1422635)
  ).toTypedDF

  val countries = Seq(
    Country("United Kingdom", "Europe", "London", 67886011, 39532),
    Country("France", "Europe", "Paris", 65273511, 39827),
    Country("USA", "North America", "Washington", 331002651, 59939),
    Country("Poland", "Europe", "Warsaw", 37846611, 13871),
    Country("Canada", "North America", "Ottawa", 37742154, 44841)
  ).toTypedDF

  countries.join(cities) // shorthand for: countries.as("countries").join(cities.as("cities"))
    .on($.countries.capital === $.cities.name)
    .select(
      $.countries.name.as("country"),
      $.continent,
      $.cities.population.as("capital population")
    ).show()

  countries.groupBy($.continent).agg(avg($.gdp).as("avg gdp")).show()

  spark.stop()



import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.shouldEqual
import java.io.ByteArrayOutputStream

class ExampleTest extends AnyFunSuite:
  test("Countries example") {
    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) { runExample() }
    val result = new String(outCapture.toByteArray)

    val expected = """|+--------------+-------------+------------------+
                      ||       country|    continent|capital population|
                      |+--------------+-------------+------------------+
                      ||United Kingdom|       Europe|           9540576|
                      ||        Canada|North America|           1422635|
                      ||        France|       Europe|          11142303|
                      ||        Poland|       Europe|           1794532|
                      ||           USA|North America|            718355|
                      |+--------------+-------------+------------------+
                      |
                      |+-------------+------------------+
                      ||    continent|           avg gdp|
                      |+-------------+------------------+
                      ||       Europe|31076.666666666668|
                      ||North America|           52390.0|
                      |+-------------+------------------+
                      |
                      |""".stripMargin

    result shouldEqual expected
  }
