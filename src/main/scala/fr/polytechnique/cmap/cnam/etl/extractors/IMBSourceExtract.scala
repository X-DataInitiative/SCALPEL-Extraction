package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Date

import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.datetime
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait IMBEventExtractor extends Serializable with IMBSourceInfo {
  def extract(r: Row) : List[Event[AnyEvent]]
}

class IMBDiagnosisEventExtractor(codes: Seq[String]) extends IMBEventExtractor {
  def extract(r: Row) : List[Event[AnyEvent]] = {
    import datetime.implicits._
    {
      val encoding = r.getAs[String](ImbDiagnosesCols.Encoding)
      val idx = r.fieldIndex(ImbDiagnosesCols.Code)
      if (encoding != "CIM10" || r.isNullAt(idx))
        None
      codes.find(r.getString(idx).startsWith(_))
    }.map{ code =>
      ImbDiagnosis(
        r.getAs[String](ImbDiagnosesCols.PatientID), code, r.getAs[Date](ImbDiagnosesCols.Date).toTimestamp)
    }.toList
  }
}

class IMBSourceExtractor(val sources : Sources, val extractors: List[IMBEventExtractor]) extends SourceExtractor with IMBSourceInfo {
  override def select() : DataFrame = {
    sources.irImb.get
  }

  def extract[A <: AnyEvent]() : Dataset[Event[A]] = {
    val df = select()
    import df.sqlContext.implicits._
    df.flatMap { r : Row => extractors.flatMap(extractor => extractor.extract(r)) }
    .asInstanceOf[Dataset[Event[A]]]
  }
}
