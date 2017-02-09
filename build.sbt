name := "SNIIRAM-flattening"

version := "1.0"

scalaVersion := "2.10.6"
val sparkVersion = "1.6.2"

parallelExecution in Test := false

test in assembly := {}

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "1.8.5" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
libraryDependencies +=   "saurfang" % "spark-sas7bdat" % "1.1.4-s_2.10"