package fr.polytechnique.cmap.cnam.flattening

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object SasConfig {

  sealed trait ExportFormatType

  object ExportFormatType  {
      case object CSV extends ExportFormatType
      case object Parquet extends ExportFormatType

      def fromString(value: String): ExportFormatType = value match {
          case "CSV" => ExportFormatType.CSV
          case "Parquet" => ExportFormatType.Parquet
        }
    }


  private lazy val conf: Config = {

    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")
    val defaultConfig = ConfigFactory.parseResources("config/sas-default.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    newConfig.withFallback(defaultConfig).resolve()
  }

  lazy val inputDirectory = conf.getString("inputDir")
  lazy val outputDirectoryCSV = conf.getString("outputDir.csv")
  lazy val outputDirectoryParquet = conf.getString("outputDir.parquet")
}
