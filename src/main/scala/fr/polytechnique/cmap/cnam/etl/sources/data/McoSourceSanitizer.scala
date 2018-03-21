package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}


private[data] class McoSourceImplicits(rawMco: DataFrame) {

  def filterSpecialHospitals: DataFrame = {
    rawMco.where(!McoSourceImplicits.ETA_NUM.isin(McoSourceImplicits.specialHospitalCodes: _*))
  }

  def filterSharedHospitalStays: DataFrame = {
    val duplicateHospitalsFilter: Column = McoSourceImplicits.SEJ_TYP.isNull or McoSourceImplicits
      .SEJ_TYP =!= "B" or (McoSourceImplicits.GRG_GHM.like("28%") and !McoSourceImplicits.GRG_GHM
      .isin(McoSourceImplicits.GRG_GHMExceptions: _*))
    rawMco.filter(duplicateHospitalsFilter)
  }

  def filterIVG: DataFrame = {
    rawMco.filter(McoSourceImplicits.GRG_GHM =!= "14Z08Z")
  }

  def filterNonReimbursedStays: DataFrame = {
    rawMco.filter(McoSourceImplicits.GHS_NUM =!= "9999")
  }

  def filterMcoCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = !McoSourceImplicits.GRG_GHM.like("90%") and McoSourceImplicits
      .NIR_RET === "0" and McoSourceImplicits.SEJ_RET === "0" and McoSourceImplicits
      .FHO_RET === "0" and McoSourceImplicits.PMS_RET === "0" and McoSourceImplicits.DAT_RET === "0"

    rawMco.filter(fictionalAndFalseHospitalStaysFilter)
  }

  def filterMcoCeCorruptedHospitalStays: DataFrame = {
    val fictionalAndFalseHospitalStaysFilter: Column = McoSourceImplicits.NIR_RET === "0" and McoSourceImplicits
      .NAI_RET === "0" and McoSourceImplicits.SEX_RET === "0" and McoSourceImplicits
      .IAS_RET === "0" and McoSourceImplicits.ENT_DAT_RET === "0"

    rawMco.filter(fictionalAndFalseHospitalStaysFilter)
  }
}


private[data] object McoSourceImplicits {

  val specialHospitalCodes = List(
    "130780521", "130783236", "130783293", "130784234", "130804297", "600100101", "690783154",
    "690784137", "690784152", "690784178", "690787478", "750041543", "750100018", "750100042",
    "750100075", "750100083", "750100091", "750100109", "750100125", "750100166", "750100208",
    "750100216", "750100232", "750100273", "750100299", "750801441", "750803447", "750803454",
    "830100558", "910100015", "910100023", "920100013", "920100021", "920100039", "920100047",
    "920100054", "920100062", "930100011", "930100037", "930100045", "940100027", "940100035",
    "940100043", "940100050", "940100068", "950100016")

  // radiotherapie & dialyse exceptions
  val GRG_GHMExceptions = List("28Z14Z","28Z15Z","28Z16Z")

  // MCO & MCO_CE shared columns
  val ETA_NUM: Column = col("ETA_NUM")
  val NIR_RET: Column = col("NIR_RET")
  val NAI_RET: Column = col("NAI_RET")
  val SEX_RET: Column = col("SEX_RET")

  // MCO exclusive columns
  val SEJ_TYP: Column = col("MCO_B__SEJ_TYP")
  val GRG_GHM: Column = col("MCO_B__GRG_GHM")
  val GHS_NUM: Column = col("MCO_B__GHS_NUM")
  val SEJ_RET: Column = col("SEJ_RET")
  val FHO_RET: Column = col("FHO_RET")
  val PMS_RET: Column = col("PMS_RET")
  val DAT_RET: Column = col("DAT_RET")


  // MCO_CE exclusive columns
  val IAS_RET: Column = col("IAS_RET")
  val ENT_DAT_RET: Column = col("ENT_DAT_RET")
}


trait McoSourceSanitizer {

  implicit def addMcoSourceSanitizerImplicits(rawMco: DataFrame) : McoSourceImplicits = {
    new McoSourceImplicits(rawMco)
  }
}
