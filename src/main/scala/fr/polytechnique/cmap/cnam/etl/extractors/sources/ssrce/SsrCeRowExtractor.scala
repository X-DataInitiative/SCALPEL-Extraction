// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.sources.ssrce

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

/**
 *  Gets the following fields for SSR_CE sourced events: patientID and start.
 */
trait SsrCeRowExtractor extends SsrCeSource with EventRowExtractor {
  override def usedColumns: List[String] = ColNames.core ++ super.usedColumns

  override def extractPatientId(row: Row): String = row.getAs[String](ColNames.PatientID)

  override def extractStart(row: Row): Timestamp = row.getAs[Timestamp](ColNames.StartDate).toTimestamp
}
