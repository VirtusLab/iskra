
ThisBuild / scalaVersion := "3.1.1-RC1"

val sparkVersion = "3.2.0"

val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkScala3 = "io.github.vincenzobaz" %% "spark-scala3" % "0.1.3-spark3.2.0-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    name := "typed-spark",
    libraryDependencies ++= Seq(
      sparkCore, sparkSql, sparkScala3
    )
  )
