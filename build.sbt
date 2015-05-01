import AssemblyKeys._

assemblySettings

name := "barebone-template"

organization := "io.prediction"

libraryDependencies ++= Seq(
  "io.prediction" %% "core" % "0.9.3-SNAPSHOT" % "provided",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "org.json4s" %% "json4s-native" % "3.2.10",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.5.1",
  "com.google.guava" % "guava" % "12.0"
)
