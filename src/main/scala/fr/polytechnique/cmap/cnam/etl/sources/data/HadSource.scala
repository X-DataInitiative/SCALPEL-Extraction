package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.{col, to_date, year}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

/**
  * Extractor class for the SSR table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
object HadSource extends DataSourceManager with HadSourceSanitizer {

  // unused for filtering
//  val ETA_NUM_EPMSI: Column = col("ETA_NUM_EPMSI")
//  val RHAD_NUM: Column = col("RHAD_NUM")
//  val DP: Column = col("HAD_B__DGN_PAL")
//  val PEC_PAL: Column = col("HAD_B_PEC_PAL")
//  val PEC_ASS: Column = col("HAD_B_PEC_ASS")
//  val DA: Column = col("HAD_D__DGN_ASS")
//  val CCAM: Column = col("HAD_A__CCAM_COD")

  val NIR_RET: Column = col("NIR_RET")
  val SEJ_RET: Column = col("SEJ_RET")
  val FHO_RET: Column = col("FHO_RET")
  val PMS_RET: Column = col("PMS_RET")
  val DAT_RET: Column = col("DAT_RET")
  
//  val ENT_DAT: Column = col("ENT_DAT")
//  val SOR_DAT: Column = col("SOR_DAT")
  val Year: Column = col("year")

  //val foreignKeys: List[String] = List("ETA_NUM_EPMSI", "RHA_NUM", "year")

  override def sanitize(rawHad: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawHad
      .filterHadCorruptedHospitalStays
  }
}
