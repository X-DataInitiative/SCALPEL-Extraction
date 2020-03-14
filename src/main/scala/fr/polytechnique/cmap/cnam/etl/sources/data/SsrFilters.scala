package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.etl.sources.data.DoublonFinessPmsi.specialHospitalCodes
import org.apache.spark.sql.{Column, DataFrame}

private[data] class SsrFilters(rawSsr: DataFrame) {
  /** Filter out Finess doublons.
    *
    * @return
    */
  def filterSpecialHospitals: DataFrame = {
    rawSsr.where(!SsrSource.ETA_NUM.isin(specialHospitalCodes: _*))
  }

  /** Filter out shared stays (between hospitals).
    *
    * @return
    */
  def filterSharedHospitalStays: DataFrame = {
    val duplicateHospitalsFilter: Column = !(SsrSource.ENT_MOD === 1 and SsrSource.SOR_MOD === 1) or
      (SsrSource.GRG_GME.like("28%") and !SsrSource.GRG_GME
      .isin(SsrFilters.GRG_GHMExceptions: _*))
    rawSsr.filter(duplicateHospitalsFilter)
  }

  /** Filter out Ssr corrupted stays as returned by the ATIH.
    *
    * @return
    */
  def filterSsrCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = !SsrSource.GRG_GME.like("90%") and SsrSource
      .NIR_RET === "0" and SsrSource.SEJ_RET === "0" and SsrSource
      .FHO_RET === "0" and SsrSource.PMS_RET === "0" and SsrSource
      .DAT_RET === "0"

    rawSsr.filter(fictionalAndFalseHospitalStaysFilter)
  }

  /** Filter out SsrCe corrupted stays as returned by the ATIH.
    *
    * @return
    */
  def filterSsrCeCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = SsrCeSource.NIR_RET === "0" and SsrCeSource
      .NAI_RET === "0" and SsrCeSource.SEX_RET === "0" and SsrCeSource
      .IAS_RET === "0" and SsrCeSource.ENT_DAT_RET === "0"

    rawSsr.filter(fictionalAndFalseHospitalStaysFilter)
  }
}

private[data] object SsrFilters {
  // radiotherapie & dialyse exceptions
  val GRG_GHMExceptions = List("28Z14Z", "28Z15Z", "28Z16Z")
}
