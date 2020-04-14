package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.sources.data.DoublonFinessPmsi.specialHospitalCodes

private[data] class SsrFilters(rawSsr: DataFrame) {
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

  /** Filter out Finess doublons.
    *
    * @return
    */
  def filterSpecialHospitals: DataFrame = {
    rawSsr.where(!SsrSource.ETA_NUM.isin(specialHospitalCodes: _*))
  }
}

private[data] object SsrFilters