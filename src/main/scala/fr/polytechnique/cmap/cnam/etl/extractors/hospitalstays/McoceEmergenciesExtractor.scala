package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, McoceEmergency}
import fr.polytechnique.cmap.cnam.etl.extractors.{AlwaysTrueStrategy, BaseExtractorCodes}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeBasicExtractor

object McoceEmergenciesExtractor extends McoCeBasicExtractor[HospitalStay] with AlwaysTrueStrategy[HospitalStay] {

  /** Checks if the passed Row has the information needed to build the Event.
    *
    * @param row The row itself.
    * @return A boolean value.
    */
  override def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(ColNames.ActCode)) && row
    .getAs[String](ColNames.ActCode).startsWith("ATU")

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractValue(row: Row): String = extractGroupId(row)

  override def columnName: String = ColNames.ActCode

  override def eventBuilder: EventBuilder = McoceEmergency

  override def usedColumns: List[String] = List(ColNames.EndDate) ++ super.usedColumns

  override def getCodes: BaseExtractorCodes = BaseExtractorCodes.empty
}
