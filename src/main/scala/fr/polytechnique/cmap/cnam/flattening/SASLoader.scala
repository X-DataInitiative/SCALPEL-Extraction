package fr.polytechnique.cmap.cnam.flattening

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, Dataset}
import com.github.saurfang.sas.spark._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering.{FilteringConfig, FlatEvent, implicits}
import fr.polytechnique.cmap.cnam.flattening.SasConfig.ExportFormatType
/**
  * Created by firas on 06/01/2017.
  */
object SASLoader extends Main{

  override def appName: String = "ImportSAS"

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def readSasAsDF(sqlContext: HiveContext, argsMap: Map[String, String] = Map()): Map[String,DataFrame] ={

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val lsFile = getListOfFiles(SasConfig.inputDirectory)
    val result = lsFile.map(file => (file.getName(),sqlContext.sasFile(file.getPath())))

    result.toMap
  }

  def writeAsCSV(ListDF: Map[String,DataFrame], outputDir : String) =
    ListDF.foreach{case(name,df) =>
      df.write.format("csv").option("header", "true").save(outputDir +"/"+ name)}

  def writeAsParquet(ListDF: Map[String,DataFrame], outputDir : String) =
    ListDF.foreach{case(name,df) =>
      df.write.parquet(outputDir +"/"+ name)}

  def ComputeDF(sqlContext: HiveContext, argsMap: Map[String, String] = Map()): List[DataFrame] = {

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val MapDF = readSasAsDF(sqlContext,argsMap)
    val exportType = ExportFormatType.fromString( argsMap.getOrElse("outputFormat","CSV"))
    exportType match {
      case ExportFormatType.CSV => writeAsCSV(MapDF,SasConfig.outputDirectoryCSV)
      case ExportFormatType.Parquet => writeAsParquet(MapDF,SasConfig.outputDirectoryParquet)
    }

    MapDF.values.toList
  }
  def run(sqlContext: HiveContext, argsMap: Map[String, String] = Map()): Option[Dataset[_]] = {

    val res = ComputeDF(sqlContext,argsMap)
    None
  }


}
