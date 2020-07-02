package fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, McoceEmergency}
import fr.polytechnique.cmap.cnam.etl.extractors.AlwaysTrueStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce.McoCeSimpleExtractor

object McoceEmergenciesExtractor extends McoCeSimpleExtractor[HospitalStay] with AlwaysTrueStrategy[HospitalStay] {

  // Extractor trait
  override def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(ColNames.ActCode)) && row
    .getAs[String](ColNames.ActCode).startsWith("ATU")

  override def getCodes: SimpleExtractorCodes = SimpleExtractorCodes.empty

  // EventRowExtractor trait
  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractValue(row: Row): String = extractGroupId(row)

  override def usedColumns: List[String] = List(ColNames.EndDate) ++ super.usedColumns

  // SimpleExtractor trait
  override def columnName: String = ColNames.ActCode

  override def eventBuilder: EventBuilder = McoceEmergency

}
