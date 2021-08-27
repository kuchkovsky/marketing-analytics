name := "marketing-analytics"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "io.spray" %%  "spray-json" % "1.3.5",
  "org.rogach" %% "scallop" % "3.5.1",
  "org.scalatest" %% "scalatest" % "3.2.3" % Test
)
