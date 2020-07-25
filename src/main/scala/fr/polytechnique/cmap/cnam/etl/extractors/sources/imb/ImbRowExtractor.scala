// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.imb

import java.sql.{Date, Timestamp}
import scala.util.Try
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor
import fr.polytechnique.cmap.cnam.util.datetime
import fr.polytechnique.cmap.cnam.util.functions.makeTS

/**
 * Gets the following fields for IMB sourced events: patientID, start, end, groupId.
 *
 * IR_IMB_R contains the Chronic Diseases Diagnoses or `ALD = Affection Longue Durée` for patients once
 * they have been exonerated for all cares related to this chronic disease.
 * It is the medical service of the health insurance that grants this ALD on the proposal of the
 * patient's GP (Medecin Traitant).
 * See the [online snds documentation for further details]
 * (https://documentation-snds.health-data-hub.fr/fiches/beneficiaires_ald.html#le-dispositif-des-ald)
 */
trait ImbRowExtractor extends ImbSource with EventRowExtractor {

  def extractEncoding(row: Row): String = row.getAs[String](ColNames.Encoding)

  override def extractPatientId(row: Row): String = row.getAs[String](ColNames.PatientID)

  override def extractStart(row: Row): Timestamp = {
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
  override def extractEnd(r: Row): Option[Timestamp] = {
    import datetime.implicits._
    Try(
    {
      val rawEndDate = r.getAs[java.util.Date](ColNames.EndDate).toTimestamp

      if (makeTS(1700, 1, 1).after(rawEndDate)) {
        None
      }
      else {
        Some(rawEndDate)
      }
    }
    ) recover {
      case _: NullPointerException => None
    }
  }.get
}


