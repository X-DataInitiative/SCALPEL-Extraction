name := "SCALPEL-Extraction"

git.baseVersion := "2.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"
val catsVersion = "2.0.0"

enablePlugins(GitVersioning)

logLevel in compile := Level.Warn
parallelExecution in Test := false
test in assembly := {}
  
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

scalacOptions := Seq("-Xmacro-settings:materialize-derivations", "-Ypartial-unification", "-deprecation")
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

val sparkDependencies = List(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

val catDependencies = List(
  "org.typelevel" %% "cats-core" % catsVersion
)

val testDependencies = List(
  "org.scalatest" %% "scalatest" % "3.1.0" % Test,  // Main scala library for testing
  "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % Test,
  "org.mockito" % "mockito-core" % "2.3.0" % Test,   // Java library for mocking
  "org.typelevel" %% "cats-laws" % catsVersion % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.3" % Test,
  "org.typelevel" %% "discipline" % "0.11.1" % Test
)

val additionalDependencies = List(
  "danielpes" % "spark-datetime-lite" % "0.2.0-s_2.11",
  "com.github.pureconfig" %% "pureconfig" % "0.12.0",
  "org.scalactic" %% "scalactic" % "3.1.0" //recommended by ScalaTest
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ additionalDependencies ++ catDependencies
