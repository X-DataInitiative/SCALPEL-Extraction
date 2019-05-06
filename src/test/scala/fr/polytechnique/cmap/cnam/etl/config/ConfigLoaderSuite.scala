package fr.polytechnique.cmap.cnam.etl.config

import java.io.File
import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig

class ConfigLoaderSuite extends FlatSpec {

  "loadConfigWithDefaults" should "read a config file and an environment of the default file and merge them" in {

    def saveConfig(config: String, path: String): Unit = {
      pureconfig.saveConfigAsPropertyFile(
        ConfigFactory.parseString(config.trim.stripMargin), Paths.get(path), true)
    }

    // Given
    val (defaultConfig, newConfig) = ("config/test/default.conf", "target/new.conf")

    case class SomeObject(someKey: String, otherKey: Int)
    case class FinalObject(anotherKey: List[Int])
    case class TestConfig(someObject: SomeObject, finalObject: FinalObject) extends StudyConfig

    saveConfig("""
      | some_object.some_key = "overriden_value"
      | final_object.another_key = [1, 1, 2]
      | final_object.inexistent_key = "ignored_value"
    """, newConfig)

    val expected =
      TestConfig(
        SomeObject(
          someKey = "overriden_value",
          otherKey = 2
        ),
        FinalObject(
          anotherKey = List(1, 1, 2)
        )
      )

    // When
    object TestLoader extends ConfigLoader
    import TestLoader._
    val result = loadConfigWithDefaults[TestConfig](newConfig, defaultConfig, "env1")

    // Then
    try assert(result == expected)
    finally {
      new File(newConfig).delete()
    }
  }
}

