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
  val MOR_PRP: Column = col("MOR_PRP")
  val ETL_AFF: Column = col("ETL_AFF")
  val MOI_ANN_SOR_SEJ: Column = col("MOI_ANN_SOR_SEJ")
  val RHS_ANT_SEJ_ENT: Column = col("RHS_ANT_SEJ_ENT")
  val FP_PEC: Column = col("FP_PEC")

  val NAI_RET: Column = col("SSR_C__NAI_RET")
  val NIR_RET: Column = col("SSR_C__NIR_RET")
  val SEX_RET: Column = col("SSR_C__SEX_RET")
  val SEJ_RET: Column = col("SSR_C__SEJ_RET")
  val FHO_RET: Column = col("SSR_C__FHO_RET")
  val PMS_RET: Column = col("SSR_C__PMS_RET")
  val DAT_RET: Column = col("SSR_C__DAT_RET")
  val SOR_DAT: Column = col("SSR_C__SOR_DAT")
  val ENT_DAT: Column = col("SSR_C__ENT_DAT")


  override def sanitize(rawSsr: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawSsr
      .filterSsrCorruptedHospitalStays
  }
}
