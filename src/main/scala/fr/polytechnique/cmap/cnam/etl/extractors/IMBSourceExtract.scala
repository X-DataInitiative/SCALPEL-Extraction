package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Date

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.datetime
import org.apache.spark.sql._

class IMBEventExtractor(codes: Seq[String]) extends Serializable with IMBSourceInfo {
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
class IMBSourceExtractor extends Serializable with IMBSourceInfo {
  def extract[A <: AnyEvent](df : DataFrame, exrs: List[IMBEventExtractor]) : Dataset[Event[Diagnosis]] = {
    import df.sqlContext.implicits._
    df.flatMap { r : Row => exrs.flatMap(ex => ex.extract(r)) }
  }
}
