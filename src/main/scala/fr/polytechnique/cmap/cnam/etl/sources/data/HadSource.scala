package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.{col, to_date, year}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

/**
  * Extractor class for the SSR table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
object HadSource extends DataSourceManager with HadSourceSanitizer {

  val ETA_NUM_EPMSI: Column = col("ETA_NUM_EPMSI")
  val NIR_RET: Column = col("NIR_RET")
  val SEJ_RET: Column = col("SEJ_RET")
  val FHO_RET: Column = col("FHO_RET")
  val PMS_RET: Column = col("PMS_RET")
  val DAT_RET: Column = col("DAT_RET")
  val Year: Column = col("year")

  override def sanitize(rawHad: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawHad
      .filterHadCorruptedHospitalStays
      .filterSpecialHospitals
  }
}
