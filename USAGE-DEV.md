## Usage (for devs)

:warning: This library is in its early stage of development - the syntax and type hierarchy might still change,
the coverage of Spark's API is far from being complete and more tests are needed.

### First steps

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

  Iskra is built with Scala 3.3.0 so it's compatible with Scala 3.3.x (LTS) and newer minor releases.
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
    * by using `.toDF` extension method on a `Seq` of case classes, e.g.
      ```scala
      Seq(Foo(1, "abc"), Foo(2, "xyz")).toDF
      ```
      Note that this variant of `.toDF` comes from `org.virtuslab.iskra.api` rather than from `spark.implicits`.

    * by taking a good old (untyped) data frame and calling `typed` extension method on it with a type parameter representing a case class, e.g.
      ```scala
      df.typed[Foo]
      ```
      In case you needed to get back to the unsafe world of untyped data frames for some reason, just call `.untyped` on a typed data frame.

5) Follow your intuition of a Spark developer :wink: This library is intended to maximally resemble the original API of Spark (e.g. by using the same names of methods, etc.) where possible, although trying to make the code feel more like regular Scala without unnecessary boilerplate but with some other syntactic improvements.

6) Look at the [examples](src/test/example/).

### Key concepts and differences to untyped Spark

Data frames created with `.toDF` or `.typed` like above are called `ClassDataFrame`s as their compile-time schema is represented by a case class. As an alternative to case classes, schemas of data frames can be expressed structurally. If this is a case, such a data frame is called a `StructDataFrame`. Some data frame operations might be available only for either `ClassDataFrame`s or `StructDataFrame` while some operations are available for both. This depends on the semantics of each operation and on implementational restrictions (which might get lifted in the future). To turn a `ClassDataFrame` into a `StructDataFrame` or vice versa use `.asStruct` or `.asClass[A]` method respectively.


When operating on a data frame, `$` represents the schema of this frame, from which columns can be selected like ordinary class memebers. So to refer to a column called `foo` instead of writing `col("foo")` or `$"foo"` write `$.foo`. If the name of a column is not a valid Scala identifier, you can use backticks, e.g. ``$.`column with spaces` ``. Similarly the syntax `$.foo.bar` can be used to refer to a column originating from a specific data frame to avoid ambiguities. This corresponds to `col("foo.bar")` or `$"foo.bar"` in vanilla Spark.


Some operations like `.select(...)` or `.agg(...)` accept potentially multiple columns as arguments. You can pass individual columns separately, like `.select($.foo, $.bar)` or you can aggregate them usings `Columns(...)`, i.e. `select(Columns($.foo, $.bar))`. `Columns` will eventually get flattened so these who syntaxes are semantically equivalent. However, `Columns(...)` syntax might come in handy e.g. if you needed to embed a block of code as an argument to `.select { ... }`, e.g.
  ```scala
  .select {
    val sum = ($.x + $.y).as("sum")
    Columns($.x, $.y, sum)
  }
  ```

The syntax for joins looks slightly more like SQL, but with dots and parentheses as for usual method calls, e.g.
  ```scala
  foos.innerJoin(bars).on($.foos.barId === $.bars.id).select(...)
  ```

  As you might have noticed above, the aliases for `foos` and `bars` were automatically inferred so you don't have to write
  ```scala
  foos.as("foos").innerJoin(bars.as("bars"))
  ```
