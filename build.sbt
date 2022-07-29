ThisBuild / scalaVersion := "3.2.0-RC3"

val sparkVersion = "3.2.0"

val sparkCore = ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13)
val sparkSql = ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13)

lazy val root = (project in file("."))
  .settings(
    name := "typed-spark",
    libraryDependencies ++= Seq(
      sparkCore, sparkSql
    ),
    scalacOptions ++= Seq("-explain") 
  )
