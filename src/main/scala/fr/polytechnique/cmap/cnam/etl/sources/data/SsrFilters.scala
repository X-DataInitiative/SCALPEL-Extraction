package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.{Column, DataFrame}

private[data] class SsrFilters(rawSsr: DataFrame) {

  def filterSsrCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = !SsrSource.GRG_GHM.like("90%") and SsrSource
      .NIR_RET === "0" and SsrSource.SEJ_RET === "0" and SsrSource
      .FHO_RET === "0" and SsrSource.PMS_RET === "0" and SsrSource.DAT_RET === "0"

    rawSsr.filter(fictionalAndFalseHospitalStaysFilter)
  }

  /* SSR CE A RAJOUTER

  def filterSsrCeCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = SsrCeSource.NIR_RET === "0" and SsrCeSource
      .NAI_RET === "0" and SsrCeSource.SEX_RET === "0" and SsrCeSource
      .IAS_RET === "0" and SsrCeSource.ENT_DAT_RET === "0"

    rawSsr.filter(fictionalAndFalseHospitalStaysFilter)
  }*/
}

private[data] object SsrFilters