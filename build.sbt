name := "PartFileCompactor"

version := "0.1"

scalaVersion := "2.11.8"

organization := "com.qliro"

val sparkVersion = "2.3.2"

val sparkDependencyScope = "provided"

parallelExecution in Test := false

fork in run := true


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope ,
  "org.apache.spark" %% "spark-sql"  % sparkVersion % sparkDependencyScope ,
  "org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope ,
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test" withSources() withJavadoc() ,
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "test" withSources() withJavadoc() ,
  "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.12.0" % "test",
  "org.yaml" % "snakeyaml" % "1.24"
)

