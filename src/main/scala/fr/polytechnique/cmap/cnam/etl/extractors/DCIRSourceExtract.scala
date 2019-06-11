package fr.polytechnique.cmap.cnam.etl.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import org.apache.spark.sql._

import scala.util.Try

class DCIRMedicalActEventExtractor(codes: Seq[String]) extends Serializable with DCIRSourceInfo {
  def extract(r: Row): List[Event[MedicalAct]] = {
    val idx = r.fieldIndex(PatientCols.CamCode)
    val foundCode = codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
    if(foundCode.isEmpty)
      List.empty
    else
      foundCode.map {code =>
        val groupID = {
          getGroupId(r) recover { case _: IllegalArgumentException => DcirAct.groupID.DcirAct }
        }.get
        List(DcirAct(
          patientID = r.getAs[String](PatientCols.PatientID),
          groupID = groupID,
          code = code,
          date = r.getAs[java.util.Date](PatientCols.Date).toTimestamp
        ))
      }.get
  }

  def getGroupId(r: Row): Try[String] = Try {
    val sector = r.getAs[Double](PatientCols.Sector)
    if (!r.isNullAt(r.fieldIndex(PatientCols.Sector)) && sector != 2) {
      DcirAct.groupID.PublicAmbulatory
    }
    else {
      if (r.isNullAt(r.fieldIndex(PatientCols.GHSCode))) {
        DcirAct.groupID.Liberal
      } else {
        // Value is not at null, it is not liberal
        lazy val ghs = r.getAs[Double](PatientCols.GHSCode)
        lazy val institutionCode = r.getAs[Double](PatientCols.InstitutionCode)
        // Check if it is a private ambulatory
        if (ghs == 0 && PrivateInstitutionCodes.contains(institutionCode)) {
          DcirAct.groupID.PrivateAmbulatory
        }
        else {
          DcirAct.groupID.Unknown
        }
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
