package fr.polytechnique.cmap.cnam.etl.extractors.acts

import java.sql.Date
import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class DCIRMedicalActEventExtractor(codes: Seq[String]) extends Serializable with MedicalActsInfo {
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

class DCIRMedicalActExtractor extends Serializable with MedicalActsInfo {
  def extract(dcir: DataFrame, exrs: List[DCIRMedicalActEventExtractor]): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._
    dcir.flatMap {r : Row => exrs.flatMap(ex => ex.extract(r)) }
  }
}

class McoCEMedicalActEventExtractor extends Serializable with MedicalActsInfo {
  final lazy val outputCols = List(
    col(MCOCECols.PatientID).as("patientID"),
    col(MCOCECols.CamCode).as("code"),
    col(MCOCECols.Date).cast(TimestampType).as("eventDate"),
    lit("ACE").as("groupID")
  )

  def correctCamCode(camCodes: Seq[String])(row: Row): Boolean = {
    val camCode = row.getAs[String](MCOCECols.CamCode)
    if (camCode != null) camCodes.map(camCode.startsWith).exists(identity) else false
  }

  def extract(mcoCE: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import mcoCE.sqlContext.implicits._
    mcoCE.select(MCOCECols.getColumns.map(col): _*).filter(correctCamCode(ccamCodes) _)
      .select(outputCols: _*).map(McoCEAct.fromRow(_))
  }
}

abstract class MCOEventExtractor(column : String, codes: List[String]) extends Serializable  {
  def extract(
    r : Row, patientID : => String, groupID : => String, eventDate : => Timestamp) : List[Event[AnyEvent]] = {
    val idx = r.fieldIndex(column)
    val code = codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
    code match {
      case Some(_) => List(get(patientID, groupID, code.get, eventDate))
      case None => List.empty
    }
  }

  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[AnyEvent]
}

class MCODiagnosisEventExtractor(column : String, codes: List[String], act : Diagnosis)
      extends MCOEventExtractor(column, codes)  {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[Diagnosis] = {
    act.apply(patientID, groupID, code, date)
  }
}
class IMBDiagnosisEventExtractor(column : String, codes: List[String], act : Diagnosis)
      extends MCOEventExtractor(column, codes) {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[Diagnosis] = {
    act.apply(patientID, groupID, code, date)
  }
}

class MCOMedicalActEventExtractor(column : String, codes: List[String], act : MedicalAct)
      extends MCOEventExtractor(column, codes) {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalAct] = {
    act.apply(patientID, groupID, code, date)
  }
}

class MCOMedicalActExtractor extends Serializable with MedicalActsInfo {
  def extract[A <: AnyEvent](df : DataFrame, exrs: List[MCOEventExtractor]) : Dataset[Event[A]] = {
    var a = MCOColsFull.GHM
    import df.sqlContext.implicits._
    df.estimateStayStartTime
      .select(MCOCols.getColumns.map(functions.col): _*)
      .flatMap { r : Row =>
        lazy val patientId = r.getAs[String](MCOCols.PatientID)
        lazy val groupId   = r.getAs[String](MCOCols.EtaNum) + "_" +
                             r.getAs[String](MCOCols.RsaNum) + "_" +
                             r.getAs[Int](MCOCols.Year).toString
        lazy val eventDate = r.getAs[Timestamp](MCOCols.EstimatedStayStart)
        exrs.flatMap(ex => ex.extract(r, patientId, groupId, eventDate))
    }.asInstanceOf[Dataset[Event[A]]]
  }
}

class IMBEventExtractor(codes: Seq[String]) extends Serializable with MedicalActsInfo {
  def extract(r: Row) : List[Event[Diagnosis]] = {
    import datetime.implicits._
    val foundCode: Option[String] = {
      val encoding = r.getAs[String](ImbDiagnosesCols.Encoding)
      val idx = r.fieldIndex(ImbDiagnosesCols.Code)
      if (encoding != "CIM10" || r.isNullAt(idx))
        None
      codes.find(r.getString(idx).startsWith(_))
    }
    foundCode match {
      case None => List.empty
      case Some(code) => List(
        ImbDiagnosis(r.getAs[String](ImbDiagnosesCols.PatientID), code, r.getAs[Date](ImbDiagnosesCols.Date).toTimestamp)
      )
    }
  }
}
class IMBExtractor extends Serializable with MedicalActsInfo {
  def extract[A <: AnyEvent](df : DataFrame, exrs: List[IMBEventExtractor]) : Dataset[Event[Diagnosis]] = {
    import df.sqlContext.implicits._
    df.flatMap { r : Row => exrs.flatMap(ex => ex.extract(r)) }
  }
}
