name := """distil"""

organization := "io.btrdb"

version := "0.1.0"

scalaVersion := "2.10.4"

libraryDependencies += "io.btrdb" %% "btrdb" % "0.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

mainClass in (Compile, run) := Some("io.btrdb.distil.Main")

scalacOptions += "-feature"
scalacOptions += "-deprecation"

scalaSource in Compile <<= baseDirectory(_ / "src" / "scala")