package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Extractor class for the MCO table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
private[sources] object Mco extends SourceReader {
  val specialHospitals = List("130780521", "130783236", "130783293", "130784234", "130804297",
    "600100101", "690783154", "690784137", "690784152", "690784178", "690787478", "750041543",
    "750100018", "750100042", "750100075", "750100083", "750100091", "750100109", "750100125",
    "750100166", "750100208", "750100216", "750100232", "750100273", "750100299", "750801441",
    "750803447", "750803454", "830100558", "910100015", "910100023", "920100013", "920100021",
    "920100039", "920100047", "920100054", "920100062", "930100011", "930100037", "930100045",
    "940100027", "940100035", "940100043", "940100050", "940100068", "950100016")

  override def read(sqlContext: SQLContext, path: String): DataFrame = {
    super.read(sqlContext, path).filter(!col("ETA_NUM").isin(specialHospitals:_*))
  }
}
