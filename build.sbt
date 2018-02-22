name := "SNIIRAM-featuring"

version := "1.0"

scalaVersion := "2.11.7"
val sparkVersion = "2.1.0"

logLevel in compile := Level.Warn
parallelExecution in Test := false
test in assembly := {}

val sparkDependencies = List(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

val testDependencies = List(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",  // Main scala library for testing
  "org.mockito" % "mockito-core" % "2.3.0" % "test"   // Java library for mocking
)

val additionalDependencies = List(
  "com.typesafe" % "config" % "1.3.1"        // Java library for HOCON configuration files
  //, "com.github.pureconfig" %% "pureconfig" % "0.9.0" // Will be used in a near future
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ additionalDependencies

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "danielpes" % "spark-datetime-lite" % "0.2.0-s_2.11"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
