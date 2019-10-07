// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Extractor class for the MCO table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
object McoSource extends DataSourceManager with McoSourceSanitizer {
  // MCO & MCO_CE shared columns
  val ETA_NUM: Column = col("ETA_NUM")
  val NIR_RET: Column = col("NIR_RET")
  val NAI_RET: Column = col("NAI_RET")
  val SEX_RET: Column = col("SEX_RET")

  // Exclusive columns
  val SEJ_TYP: Column = col("MCO_B__SEJ_TYP")
  val GRG_GHM: Column = col("MCO_B__GRG_GHM")
  val GHS_NUM: Column = col("MCO_B__GHS_NUM")
  val SEJ_RET: Column = col("SEJ_RET")
  val FHO_RET: Column = col("FHO_RET")
  val PMS_RET: Column = col("PMS_RET")
  val DAT_RET: Column = col("DAT_RET")

  override def sanitize(rawMco: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawMco
      .filterSpecialHospitals
      .filterSharedHospitalStays
      .filterIVG
      .filterNonReimbursedStays
      .filterMcoCorruptedHospitalStays
  }
}
