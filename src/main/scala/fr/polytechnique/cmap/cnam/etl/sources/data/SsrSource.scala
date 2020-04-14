package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Extractor class for the SSR table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
object SsrSource extends DataSourceManager with SsrSourceSanitizer {

  val ETA_NUM: Column = col("ETA_NUM")
  val RHA_NUM: Column = col("RHA_NUM")
  val RHS_NUM: Column = col("RHS_NUM")
  val MOR_PRP: Column = col("SSR_B__MOR_PRP")
  val ETL_AFF: Column = col("SSR_B__ETL_AFF")
  val MOI_ANN_SOR_SEJ: Column = col("SSR_B__MOI_ANN_SOR_SEJ")
  val RHS_ANT_SEJ_ENT: Column = col("SSR_B__RHS_ANT_SEJ_ENT")
  val FP_PEC: Column = col("SSR_B__FP_PEC")
  val GRG_GME: Column = col("SSR_B__GRG_GME")
  val NIR_RET: Column = col("NIR_RET")
  val SEJ_RET: Column = col("SEJ_RET")
  val FHO_RET: Column = col("FHO_RET")
  val PMS_RET: Column = col("PMS_RET")
  val DAT_RET: Column = col("DAT_RET")
  val ENT_DAT: Column = col("ENT_DAT")
  val SOR_DAT: Column = col("SOR_DAT")
  val Year: Column = col("year")

  override val EXE_SOI_DTD: Column = col("EXE_SOI_DTD")

  val foreignKeys: List[String] = List("ETA_NUM", "RHA_NUM", "year")

  override def sanitize(rawSsr: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawSsr
      .filterSpecialHospitals
      .filterSsrCorruptedHospitalStays
  }
}
