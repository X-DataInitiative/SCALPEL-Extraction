package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

abstract class MCOEventExtractor(column : String, codes: List[String]) extends Serializable with MCOSourceInfo {
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
  def columns() : List[String] = MCOCols.getColumns.toList
}

class MCODiagnosisEventExtractor(column : String, codes: List[String], act : Diagnosis)
      extends MCOEventExtractor(column, codes)  {
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

class ClassificationEventExtractor(column : String, codes: List[String], act : Classification)
      extends MCOEventExtractor(column, codes) {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[Classification] = {
    act.apply(patientID, groupID, code, date)
  }
  override def columns() : List[String] = List.concat(MCOCols.getColumns.toList,(List(MCOColsFull.GHM)))
}

class MCOSourceExtractor extends Serializable with MCOSourceInfo {
  def extract[A <: AnyEvent](df : DataFrame, exrs: List[MCOEventExtractor]) : Dataset[Event[A]] = {
    val qcols = exrs.flatMap(exr => exr.columns()).distinct
    import df.sqlContext.implicits._
    df.estimateStayStartTime
      .select(qcols.map(functions.col): _*)
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



