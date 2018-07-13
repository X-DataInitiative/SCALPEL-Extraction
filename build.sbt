name := "SNIIRAM-featuring"

version := "1.0"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"

logLevel in compile := Level.Warn
parallelExecution in Test := false
test in assembly := {}
  
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
scalacOptions := Seq("-Xmacro-settings:materialize-derivations")
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

val sparkDependencies = List(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

val testDependencies = List(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",  // Main scala library for testing
  "org.mockito" % "mockito-core" % "2.3.0" % "test"   // Java library for mocking
)

val additionalDependencies = List(
  "danielpes" % "spark-datetime-lite" % "0.2.0-s_2.11",
  "com.github.pureconfig" %% "pureconfig" % "0.9.0"
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ additionalDependencies

