package fr.polytechnique.cmap.cnam.utilities

import scala.collection.JavaConversions._
import com.typesafe.config._

/**
  * Created by burq on 07/07/16.
  */
object FlatteningConfig {

  // todo :
  // here we need to raise a bunch of errors when the conf file is not well formatted

  private var Conf: Config = ConfigFactory.load()

  private val EnvironmentConfigKey: String = "environment"
  private val EnvironmentName: String = Conf.getString(EnvironmentConfigKey)
  private val OutputPathConfigKey: String = ".output_path"
  private val TableConfigKey: String = ".tables"
  private val JoinConfigKey: String = ".join"


  val outputPath = Conf.getString(EnvironmentName + OutputPathConfigKey)

  val tablesConfig : List[Config] = {
    Conf.getConfList(EnvironmentName + TableConfigKey)
  }

  val joinTablesConfig : List[Config] = {
    Conf.getConfList(EnvironmentName + JoinConfigKey)
  }

  implicit class TableConfig(config: Config) {

    def getConfList(path: String): List[Config] = {
      config.getConfigList(path).toList
    }

    def getStrList(path: String): List[String] = {
      config.getStringList(path).toList
    }

    def name: String = {
      config.getString("name")
    }

    def columns: List[String] = {
      config.getStrList("columns")
    }

    def paths: List[String] = {
      config.getStrList("paths")
    }

    def tableName: String = {
      config.getString("output.tableName")
    }

    def key: String = {
      config.getString("output.key")
    }

    def mainTable: String = {
      config.getString("main_table")
    }

    def tables: List[String] = {
      config.getStrList("tables")
    }


    def dataType: String = {
      config.getString("type")
    }

    def fields: List[Config] = {
      config.getConfList("fields")
    }

  }

  def getTableConfig(name: String): Config = {
    Conf.getConfigList(EnvironmentName + TableConfigKey).filter(_.name == name).head
  }

  def addTable(other: Config): Unit = {
    Conf = other.withFallback(Conf)
  }


}
