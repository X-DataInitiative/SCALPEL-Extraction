// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import scala.util.Try

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{DataFrame, Row}

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, ImbDiagnosis}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions.makeTS


object ImbDiagnosisExtractor extends Extractor[Diagnosis] with ImbSource {

  override def isInExtractorScope(row: Row): Boolean = {
    lazy val idx = row.fieldIndex(ColNames.Code)
    getEncoding(row) == "CIM10" || !row.isNullAt(idx)
  }

  override def isInStudy(codes: Set[String])
                        (row: Row): Boolean = codes.exists(getCode(row).startsWith(_))

  override def builder(row: Row): Seq[Event[Diagnosis]] =
    Seq(ImbDiagnosis(getPatientID(row), getCode(row), getEventDate(row), getEventEnd(row)))

  override def getInput(sources: Sources): DataFrame = sources.irImb.get
}

/** IR_IMB_R contains the Chronic Diseases diagnoses (ALD = Affection Longue Duree) for patients once
  * they have been exonerated for all cares related to this Chronic Disease.
  * It is the medical service of the health insurance  that grants this ALD on the proposal of the
  * patient's main physician (Medecin Traitant).
  * See the [online snds documentation for further details](https://documentation-snds.health-data-hub.fr/fiches/beneficiaires_ald.html#le-dispositif-des-ald)
  *
  */
trait ImbSource extends Serializable {

  lazy val getCode = (row: Row) => row.getAs[String](ColNames.Code)

  def getEncoding(row: Row): String = row.getAs[String](ColNames.Encoding)

  def getPatientID(row: Row): String = row.getAs[String](ColNames.PatientID)

  def getEventDate(row: Row): Timestamp = {
    import datetime.implicits._

    row.getAs[Date](ColNames.Date).toTimestamp
  }

  /**
    * The End date of the ALD is not always written. It can takes the value 1600-01-01 which
    * corresponds to a None value (not set) that we convert to None.
    * See the CNAM documentation [available here](https://documentation-snds.health-data-hub.fr/fiches/beneficiaires_ald.html#annexe)
    *
    * @param r
    * @return
    */
  def getEventEnd(r: Row): Option[Timestamp] = {
    Try({
      val rawEndDate = r.getAs[java.util.Date](ColNames.EndDate).toTimestamp

      if (makeTS(1700, 1 ,1).after(rawEndDate)){
        None
      }
      else {
        Some(rawEndDate)
      }
    }) recover {
    case _: NullPointerException => None
    }
    }.get

  final object ColNames extends Serializable {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val Encoding = "MED_NCL_IDT"
    final lazy val Code = "MED_MTF_COD"
    final lazy val Date = "IMB_ALD_DTD"
    final lazy val EndDate = "IMB_ALD_DTF"
  }

}