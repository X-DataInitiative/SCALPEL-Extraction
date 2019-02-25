package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import java.sql.Date
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, ImbDiagnosis}
import fr.polytechnique.cmap.cnam.util.datetime

@deprecated("I said so")
private[diagnoses] object ImbDiagnoses {

  final object ColNames {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val Encoding = "MED_NCL_IDT"
    final lazy val Code = "MED_MTF_COD"
    final lazy val Date = "IMB_ALD_DTD"
  }

  def eventFromRow(codes: Seq[String])(r: Row): Option[Event[Diagnosis]] = {

    import datetime.implicits._

    val foundCode: Option[String] = {
      val encoding = r.getAs[String](ColNames.Encoding)
      val idx = r.fieldIndex(ColNames.Code)

      if (encoding != "CIM10" || r.isNullAt(idx)) {
        None
      }
      else {
        codes.find(r.getString(idx).startsWith(_))
      }
    }

    foundCode match {
      case None => None
      case Some(code) => Some(
        ImbDiagnosis(r.getAs[String](ColNames.PatientID), code, r.getAs[Date](ColNames.Date).toTimestamp)
      )
    }
  }

  def extract(imb: DataFrame, codes: Seq[String]): Dataset[Event[Diagnosis]] = {

    import imb.sqlContext.implicits._

    if(codes.isEmpty){
      return imb.sqlContext.sparkSession.emptyDataset[Event[Diagnosis]]
    }

    imb.flatMap(eventFromRow(codes))
  }
}