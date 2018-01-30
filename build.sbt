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
  "org.json4s" %% "json4s-native" % "3.5.3", // Scala library for JSON parsing and serialization
  "com.typesafe" % "config" % "1.3.1"        // Java library for HOCON configuration files
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ additionalDependencies

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
