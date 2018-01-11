package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

private[acts] object DcirMedicalActs {

  final object ColNames {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val CamCode = "ER_CAM_F__CAM_PRS_IDE"
    final lazy val GHSCode = "ER_ETE_F__ETE_GHS_NUM"
    final lazy val InstitutionCode = "ER_ETE_F__ETE_TYP_COD"
    final lazy val Date = "EXE_SOI_DTD"

    def allCols = List(PatientID, CamCode, GHSCode, InstitutionCode, Date)
  }

  final val PrivateInstitutionCodes = List(4, 5, 6, 7)

  def extract(dcir: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._
    dcir.flatMap(medicalActFromRow(ccamCodes))
  }

  def medicalActFromRow(ccamCodes: Seq[String])(r: Row): Option[Event[MedicalAct]] = {

    def noNulls: Boolean = {
      ColNames.allCols.forall { colName =>
        !r.isNullAt(r.fieldIndex(colName))
      }
    }

    val foundCode: Option[String] = ccamCodes.find {
      val idx = r.fieldIndex(ColNames.CamCode)
      !r.isNullAt(r.fieldIndex(ColNames.CamCode)) && r.getString(idx).startsWith(_)
    }

    foundCode match {
      case None => None
      case Some(code) =>
        lazy val groupID = getGroupId(r)
        lazy val act = Some(
          DcirAct(
            patientID = r.getAs[String](ColNames.PatientID),
            groupID = groupID,
            code = code,
            date = r.getAs[java.util.Date](ColNames.Date).toTimestamp)
        )
        groupID match {
          case DcirAct.groupID.Liberal => act
          case  _ if noNulls  => act
          case _ => None
        }
    }
  }

  def getGroupId(r: Row): String = {
    if(r.isNullAt(r.fieldIndex(ColNames.GHSCode)))
      DcirAct.groupID.Liberal
    else {
      val ghs = getGHS(r)
      val code = getInstitutionCode(r)
      if (ghs != 0) {
        DcirAct.groupID.PrivateHospital
      }
      else if (PrivateInstitutionCodes.contains(code)) {
        DcirAct.groupID.PrivateAmbulatory
      }
      else {
        DcirAct.groupID.PublicAmbulatory
      }
    }
  }

  def getGHS(r: Row): Double = {
    r.getAs[Double](ColNames.GHSCode)
  }

  def getInstitutionCode(r: Row): Double = {
    r.getAs[Double](ColNames.InstitutionCode)
  }
}
