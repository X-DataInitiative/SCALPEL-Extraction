package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.year

/**
  * Extractor class for the SSR table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
object SsrSource extends DataSourceManager with SsrSourceSanitizer {

  val ETA_NUM: Column = col("ETA_NUM")
  val RHA_NUM: Column = col("RHA_NUM")
  val RHS_NUM: Column = col("RHS_NUM")
  val MOR_PRP: Column = col("MOR_PRP")
  val ETL_AFF: Column = col("ETL_AFF")
  val MOI_ANN_SOR_SEJ: Column = col("MOI_ANN_SOR_SEJ")
  val RHS_ANT_SEJ_ENT: Column = col("RHS_ANT_SEJ_ENT")
  val FP_PEC: Column = col("FP_PEC")

  val NIR_RET: Column = col("SSR_C__NIR_RET")
  val SEJ_RET: Column = col("SSR_C__SEJ_RET")
  val FHO_RET: Column = col("SSR_C__FHO_RET")
  val PMS_RET: Column = col("SSR_C__PMS_RET")
  val DAT_RET: Column = col("SSR_C__DAT_RET")
  val ENT_DAT: Column = col("SSR_C__ENT_DAT")
  val SOR_DAT: Column = col("SSR_C__SOR_DAT")
  val Year: Column = col("year")

  val foreignKeys: List[String] = List("ETA_NUM", "RHA_NUM", "year")

  override def sanitize(rawSsr: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawSsr
      .filterSsrCorruptedHospitalStays
  }

  def readAnnotateJoin(sqlContext: SQLContext, paths: List[String], joinedTableName: String): DataFrame  = {
    val ssrSej = sqlContext.read.parquet(paths(0))
    val ssrC = sqlContext.read.parquet(paths(1))
    ssrSej.join(
      ssrC.addPrefixYear(joinedTableName, foreignKeys), foreignKeys, "left_outer")
  }

  implicit class TableHelper(df: DataFrame) {

    def addPrefixYear(prefix: String, except: List[String]): DataFrame = {
      val renamedColumns = df.columns.map {
        case colName if !except.contains(colName) => prefix + "__" + colName
        case keyCol => keyCol
      }
      df.toDF(renamedColumns: _*).withColumn("year", year(to_date(col("SSR_C__EXE_SOI_DTD"))))
    }
  }
}
