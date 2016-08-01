name := "SNIIRAM-flattening"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
