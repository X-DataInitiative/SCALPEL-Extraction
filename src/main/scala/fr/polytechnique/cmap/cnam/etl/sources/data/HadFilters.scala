package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.{Column, DataFrame}

private[data] class HadFilters(rawHad: DataFrame) {

  /** Removing return codes which significate error in the PMSI
   *
   * This is a classic filter for all PMSI products. Other filters may be implemented in the future.
   * */
  def filterHadCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = HadSource
      .NIR_RET === "0" and HadSource.SEJ_RET === "0" and HadSource
      .FHO_RET === "0" and HadSource.PMS_RET === "0" and HadSource
      .DAT_RET === "0"

    rawHad.filter(fictionalAndFalseHospitalStaysFilter)
  }

}

