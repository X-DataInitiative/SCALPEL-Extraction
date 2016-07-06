name := "SNIIRAM-flattening"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.apache.parquet" % "parquet-scala_2.10" % "1.8.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"