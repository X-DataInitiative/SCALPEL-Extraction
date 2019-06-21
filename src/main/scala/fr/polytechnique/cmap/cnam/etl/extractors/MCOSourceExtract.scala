package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp
import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources

abstract class MCOEventExtractor(column: String, codes: List[String]) extends Serializable with MCOSourceInfo {

  def extract(r: Row, patientID: => String, groupID: => String, eventDate: => Timestamp): List[Event[AnyEvent]] = {
    val idx = r.fieldIndex(column)
    codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
      .map(c => get(patientID, groupID, c, eventDate))
      .toList
  }

  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[AnyEvent]

  def columns(): List[String] = MCOCols.getColumns.toList
}

class MCODiagnosisEventExtractor(column: String, codes: List[String], act: Diagnosis)
  extends MCOEventExtractor(column, codes) {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[Diagnosis] = {
    act.apply(patientID, groupID, code, date)
  }
}

class MCOMedicalActEventExtractor(column: String, codes: List[String], act: MedicalAct)
  extends MCOEventExtractor(column, codes) {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalAct] = {
    act.apply(patientID, groupID, code, date)
  }
}

class ClassificationEventExtractor(column: String, codes: List[String], act: Classification)
  extends MCOEventExtractor(column, codes) {
  def get(patientID: String, groupID: String, code: String, date: Timestamp): Event[Classification] = {
    act.apply(patientID, groupID, code, date)
  }

  override def columns: List[String] = List.concat(MCOCols.getColumns.toList, List(MCOColsFull.GHM))
}

class MCOSourceExtractor(val sources : Sources, val extractors: List[MCOEventExtractor]) extends SourceExtractor with MCOSourceInfo {
  val qcols = extractors.flatMap(extractor => extractor.columns()).distinct
  override def select() : DataFrame = {
    val mco = sources.mco.get
    import mco.sqlContext.implicits._
    mco.estimateStayStartTime.select(qcols.map(functions.col): _*)
  }

  override def extract[A <: AnyEvent]() : Dataset[Event[A]] = {
    val df = select()
    import df.sqlContext.implicits._
    df.flatMap { r : Row =>
        lazy val patientId = r.getAs[String](MCOCols.PatientID)
        lazy val groupId   = r.getAs[String](MCOCols.EtaNum) + "_" +
                             r.getAs[String](MCOCols.RsaNum) + "_" +
                             r.getAs[Int](MCOCols.Year).toString
        lazy val eventDate = r.getAs[Timestamp](MCOCols.EstimatedStayStart)
        extractors.flatMap(extractor => extractor.extract(r, patientId, groupId, eventDate))
    }.asInstanceOf[Dataset[Event[A]]]
  }
}

