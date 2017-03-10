package fr.polytechnique.cmap.cnam.flattening

import java.io.File
import org.apache.spark.sql.{DataFrame, Dataset}
import com.github.saurfang.sas.spark._
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.SasConfig.ExportFormatType

object SASLoader extends Main {

  val appName: String = "ImportSAS"

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def readSasAsDF(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Map[String,DataFrame] ={

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

  def ComputeDF(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): List[DataFrame] = {

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
  def run(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Option[Dataset[_]] = {

    val res = ComputeDF(sqlContext,argsMap)
    None
  }


}
