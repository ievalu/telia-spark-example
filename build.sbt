ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "telia-spark-example"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-sql"  % "3.2.2",
  "org.scalatest"    %% "scalatest"  % "3.2.15" % Test
)
