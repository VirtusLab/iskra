// ThisBuild / scalaVersion := "3.1.2"
ThisBuild / scalaVersion := "3.2.0-RC1-bin-20220611-0260d75-NIGHTLY" // experimental code completions

val sparkVersion = "3.2.0"

val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkScala3 = "io.github.vincenzobaz" %% "spark-scala3" % "0.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "typed-spark",
    libraryDependencies ++= Seq(
      sparkCore, sparkSql, sparkScala3
    ),
    //run / fork := true
  )
