package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

private[acts] object DcirMedicalActs {

  final object ColNames {
    lazy val PatientID: String = "NUM_ENQ"
    lazy val CamCode: String = "ER_CAM_F__CAM_PRS_IDE"
    lazy val GHSCode: String = "ER_ETE_F__ETE_GHS_NUM"
    lazy val InstitutionCode: String = "ER_ETE_F__ETE_TYP_COD"
    lazy val Sector: String = "ER_ETE_F__PRS_PPU_SEC"
    lazy val Date: String = "EXE_SOI_DTD"
  }

  final val PrivateInstitutionCodes = List(4, 5, 6, 7)

  def extract(dcir: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._
    dcir.flatMap(medicalActFromRow(ccamCodes))
  }

  def medicalActFromRow(ccamCodes: Seq[String])(r: Row): Option[Event[MedicalAct]] = {

    val foundCode: Option[String] = ccamCodes.find {
      val idx = r.fieldIndex(ColNames.CamCode)
      !r.isNullAt(r.fieldIndex(ColNames.CamCode)) && r.getString(idx).startsWith(_)
    }

    foundCode match {
      case None => None
      case Some(code) =>
        val groupID = getGroupId(r)
        groupID.map { groupIDValue =>
          DcirAct(
            patientID = r.getAs[String](ColNames.PatientID),
            groupID = groupIDValue,
            code = code,
            date = r.getAs[java.util.Date](ColNames.Date).toTimestamp
          )
        }
    }
  }

  def getGroupId(r: Row): Option[String] = {

    // First delete Public stuff
    if (!r.isNullAt(r.fieldIndex(ColNames.Sector)) && getSector(r) != 2) {
      None
    }
    else {
      // If the value is at null, then it is liberal
      if(r.isNullAt(r.fieldIndex(ColNames.GHSCode)))
        Some(DcirAct.groupID.Liberal)
      else {
        // Value is not at null, it is not liberal
        val ghs = getGHS(r)
        val code = getInstitutionCode(r)
        // Check if it is a private ambulatory
        if (ghs == 0 && PrivateInstitutionCodes.contains(code)) {
          Some(DcirAct.groupID.PrivateAmbulatory)
        }
        else None // Non-liberal, non-Private ambulatory and non-public
      }
    }
  }

  def getGHS(r: Row): Double = {
    r.getAs[Double](ColNames.GHSCode)
  }

  def getInstitutionCode(r: Row): Double = {
    r.getAs[Double](ColNames.InstitutionCode)
  }

  def getSector(r: Row): Double = {
    r.getAs[Double](ColNames.Sector)
  }
}
