package fr.polytechnique.cmap.cnam.etl.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import org.apache.spark.sql._

import scala.util.Try

class DCIRMedicalActEventExtractor(codes: Seq[String]) extends Serializable with DCIRSourceInfo {
  def extract(r: Row): List[Event[MedicalAct]] = {
    val idx = r.fieldIndex(PatientCols.CamCode)
    val foundCode = codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
    foundCode match {
      case None => List.empty
      case Some(code) =>
        val groupID = {
          getGroupId(r) recover {
            case _:IllegalArgumentException => Some(DcirAct.groupID.DcirAct)
          }
        }
        groupID.get match {
          case Some(_) =>
            List(DcirAct(
              patientID = r.getAs[String](PatientCols.PatientID),
              groupID = groupID.get.get,
              code = code,
              date = r.getAs[java.util.Date](PatientCols.Date).toTimestamp
            ))
          case None => List.empty
        }
    }
  }

  def getGroupId(r: Row): Try[Option[String]] = Try {
    val sector = r.getAs[Double](PatientCols.Sector)
    // First delete Public stuff
    if (!r.isNullAt(r.fieldIndex(PatientCols.Sector)) && sector != 2) {
      None
    }
    else {
      // If the value is at null, then it is liberal
      if(r.isNullAt(r.fieldIndex(PatientCols.GHSCode)))
        Some(DcirAct.groupID.Liberal)
      else {
        // Value is not at null, it is not liberal
        val ghs = r.getAs[Double](PatientCols.GHSCode)
        val code = r.getAs[Double](PatientCols.InstitutionCode)
        // Check if it is a private ambulatory
        if (ghs == 0 && PrivateInstitutionCodes.contains(code)) {
          Some(DcirAct.groupID.PrivateAmbulatory)
        }
        else None // Non-liberal, non-Private ambulatory and non-public
      }
    }
  }
}

class DCIRSourceExtractor extends Serializable with DCIRSourceInfo {
  def extract(dcir: DataFrame, exrs: List[DCIRMedicalActEventExtractor]): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._
    dcir.flatMap {r : Row => exrs.flatMap(ex => ex.extract(r)) }
  }
}
