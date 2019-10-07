// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.{Column, DataFrame}

private[data] class McoFilters(rawMco: DataFrame) {

  def filterSpecialHospitals: DataFrame = {
    rawMco.where(!McoSource.ETA_NUM.isin(McoFilters.specialHospitalCodes: _*))
  }

  def filterSharedHospitalStays: DataFrame = {
    val duplicateHospitalsFilter: Column = McoSource.SEJ_TYP.isNull or McoSource
      .SEJ_TYP =!= "B" or (McoSource.GRG_GHM.like("28%") and !McoSource.GRG_GHM
      .isin(McoFilters.GRG_GHMExceptions: _*))
    rawMco.filter(duplicateHospitalsFilter)
  }

  def filterIVG: DataFrame = {
    rawMco.filter(McoSource.GRG_GHM =!= "14Z08Z")
  }

  def filterNonReimbursedStays: DataFrame = {
    rawMco.filter(McoSource.GHS_NUM =!= "9999")
  }

  def filterMcoCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = !McoSource.GRG_GHM.like("90%") and McoSource
      .NIR_RET === "0" and McoSource.SEJ_RET === "0" and McoSource
      .FHO_RET === "0" and McoSource.PMS_RET === "0" and McoSource.DAT_RET === "0"

    rawMco.filter(fictionalAndFalseHospitalStaysFilter)
  }

  def filterMcoCeCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = McoCeSource.NIR_RET === "0" and McoCeSource
      .NAI_RET === "0" and McoCeSource.SEX_RET === "0" and McoCeSource
      .IAS_RET === "0" and McoCeSource.ENT_DAT_RET === "0"

    rawMco.filter(fictionalAndFalseHospitalStaysFilter)
  }
}

private[data] object McoFilters {

  val specialHospitalCodes = List(
    "130780521", "130783236", "130783293", "130784234", "130804297", "600100101", "690783154",
    "690784137", "690784152", "690784178", "690787478", "750041543", "750100018", "750100042",
    "750100075", "750100083", "750100091", "750100109", "750100125", "750100166", "750100208",
    "750100216", "750100232", "750100273", "750100299", "750801441", "750803447", "750803454",
    "830100558", "910100015", "910100023", "920100013", "920100021", "920100039", "920100047",
    "920100054", "920100062", "930100011", "930100037", "930100045", "940100027", "940100035",
    "940100043", "940100050", "940100068", "950100016"
  )

  // radiotherapie & dialyse exceptions
  val GRG_GHMExceptions = List("28Z14Z", "28Z15Z", "28Z16Z")

}
