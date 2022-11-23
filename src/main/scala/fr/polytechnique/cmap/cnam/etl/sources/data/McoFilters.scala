// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.sources.data.DoublonFinessPmsi.specialHospitalCodes

private[data] class McoFilters(rawMco: DataFrame) {
  /** Filter out Finess doublons from APHP, APHM and HCL
    *
    * @return
    */
  def filterSpecialHospitals: DataFrame = {
    rawMco.where(!McoSource.ETA_NUM.isin(specialHospitalCodes: _*))
  }

  /** Filter out shared stays (between hospitals).
    *
    * @return
    */
  def filterSharedHospitalStays: DataFrame = {
    val duplicateHospitalsFilter: Column = McoSource.SEJ_TYP.isNull or
      !(McoSource.ENT_MOD === 1 and McoSource.SOR_MOD === 1) or (McoSource.GRG_GHM.like("28%") and !McoSource.GRG_GHM
      .isin(McoFilters.GRG_GHMExceptions: _*))
    rawMco.filter(duplicateHospitalsFilter)
  }

  /** Filter out induced abortion (IVG).
    *
    * @return
    */
  def filterIVG: DataFrame = {
    rawMco.filter(McoSource.GRG_GHM =!= "14Z08Z")
  }

  /** Filter out non reimbursed stays.
    *
    * @return
    */
  def filterNonReimbursedStays: DataFrame = {
    rawMco.filter(McoSource.GHS_NUM =!= "9999")
  }

  /** Filter out Mco corrupted stays as returned by the ATIH.
    *
    * @return
    */
  def filterMcoCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = !McoSource.GRG_GHM.like("90%") and McoSource
      .NIR_RET === "0" and McoSource.SEJ_RET === "0" and McoSource
      .FHO_RET === "0" and McoSource.PMS_RET === "0" and McoSource.DAT_RET === "0"

    rawMco.filter(fictionalAndFalseHospitalStaysFilter)
  }

  /** Filter out McoCe corrupted stays as returned by the ATIH.
    *
    * @return
    */
  def filterMcoCeCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = McoCeSource.NIR_RET === "0" and McoCeSource
      .NAI_RET === "0" and McoCeSource.SEX_RET === "0" and McoCeSource
      .IAS_RET === "0" and McoCeSource.ENT_DAT_RET === "0"

    rawMco.filter(fictionalAndFalseHospitalStaysFilter)
  }
}

private[data] object McoFilters {
  // radiotherapie & dialyse exceptions
  val GRG_GHMExceptions = List("28Z14Z", "28Z15Z", "28Z16Z")
}
