package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.{Column, DataFrame}

private[data] class HadFilters(rawHad: DataFrame) {

  def filterHadCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = HadSource
      .NIR_RET === "0" and HadSource.SEJ_RET === "0" and HadSource
      .FHO_RET === "0" and HadSource.PMS_RET === "0" and HadSource
      .DAT_RET === "0"

    rawHad.filter(fictionalAndFalseHospitalStaysFilter)
  }

  /* HAD CE A RAJOUTER

  def filterHadCeCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = HadCeSource.NIR_RET === "0" and HadCeSource
      .NAI_RET === "0" and HadCeSource.SEX_RET === "0" and HadCeSource
      .IAS_RET === "0" and HadCeSource.ENT_DAT_RET === "0"

    rawHad.filter(fictionalAndFalseHospitalStaysFilter)
  }*/
}

