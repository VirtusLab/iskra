## Usage (for devs)

:warning: This library is in its early stage of development - the syntax and type hierarchy might still change,
the coverage of Spark's API is far from being complete and more tests are needed.

1) Add Iskra as a dependency to your project, e.g.

* in a file compiled with Scala CLI:
  ```scala
  //> using lib "org.virtuslab::iskra:0.0.4-SNAPSHOT"
  ```

* when starting Scala CLI REPL:
  ```shell
  scala-cli repl --dep org.virtuslab::iskra:0.0.4-SNAPSHOT
  ```

* in `build.sbt` in an sbt project:
  ```scala
  libraryDependencies += "org.virtuslab" %% "iskra" % "0.0.4-SNAPSHOT"
  ```

Iskra is built with Scala 3.1.3 so it's compatible with Scala 3.1.x and newer minor releases (starting from 3.2.0 you'll get code completions for names of columns in REPL and Metals!).
Iskra transitively depends on Spark 3.2.0.

2) Import the basic definitions from the API
    ```scala
    import org.virtuslab.iskra.api.*
    ```

3) Get a Spark session, e.g.
    ```scala
    given spark: SparkSession =
      SparkSession
        .builder()
        .master("local")
        .appName("my-spark-app")
        .getOrCreate()
    ```

4) Create a typed data frame in either of the two ways:
    a) by using `toDF` extension method on a `Seq` of case classes, e.g.
      ```scala
      Seq(Foo(1, "abc"), Foo(2, "xyz")).toDF
      ```
      Note that this variant of `toDF` comes from `org.virtuslab.iskra.api` rather than from `spark.implicits`.

    b) by taking a good old (untyped) data frame and calling `typed` extension method on it with a type parameter representing a case class, e.g.
      ```scala
      df.typed[Foo]
      ```
      In case you needed to get back to the unsafe world of untyped data frames for some reason, just call `.untyped` on a typed data frame.

5) Data frames created as above are called `ClassDataFrame`s as their compile-time schema is represented by a case class. To perform most of operations (e.g. selections, joins, etc.) on such a data frame you'll need to turn it into a structural data frame (`StructDataFrame`) first by calling `.asStruct` on it. (This restriction might be loosened in the future.)

6) On the other hand, some operations (like `.collect()`), can be performed only on a `ClassDataFrame` because they require a specific case class model to be known at compile time. To transform a `StructDataFrame` back to a `ClassDataFrame` with a given case class `A` as model use `.asClass[A]`. 

7) From now on, just try to follow your intuition of a Spark developer :wink: 

    This library is intended to maximally resemble the original API of Spark (e.g. by using the same names of methods, etc.) where possible, although trying to make the code feel more like regular Scala without unnecessary boilerplate and adding some other syntactic improvements.

    Most important differences:
    * Refer to columns (also with prefixes specifying the alias for a dataframe in case of ambiguities) simply with `$.foo.bar` instead of `$"foo.bar"` or `col("foo.bar")`. Use backticks when necessary, e.g. ``$.`column with spaces` ``.
    * From inside of `.select(...)` or `.select{...}` you should return something that is a named column or a tuple of named columns. Because of how Scala syntax works you can write simply `.select($.x, $.y)` instead of `select(($.x, $.y))`. With braces you can compute intermediate values like
        ```scala
        .select {
          val sum = ($.x + $.y).as("sum")
          ($.x, $.y, sum)
        }
        ```

    *  Syntax for joins looks slightly more like SQL, but with dots and parentheses as for usual method calls, e.g.
        ```scala
        foos.innerJoin(bars).on($.foos.barId === $.bars.id).select(...)
        ```

    * As you might have noticed above, the aliases for `foos` and `bars` were automatically inferred

8) For reference look at the [examples](src/test/example/) and the [API docs](https://virtuslab.github.io/iskra/) 
