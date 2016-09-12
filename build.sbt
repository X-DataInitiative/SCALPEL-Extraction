name := "SNIIRAM-flattening"

version := "1.0"

scalaVersion := "2.10.6"

parallelExecution in Test := false

test in assembly := {}

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "1.8.5" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"