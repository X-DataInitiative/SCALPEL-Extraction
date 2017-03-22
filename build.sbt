name := "SNIIRAM-flattening"

version := "1.0"

scalaVersion := "2.11.7"
val sparkVersion = "2.1.0"

logLevel in compile := Level.Warn
parallelExecution in Test := false
test in assembly := {}

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "2.3.0" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
libraryDependencies += "saurfang" % "spark-sas7bdat" % "1.1.5-s_2.11"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "ggasoftware", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
