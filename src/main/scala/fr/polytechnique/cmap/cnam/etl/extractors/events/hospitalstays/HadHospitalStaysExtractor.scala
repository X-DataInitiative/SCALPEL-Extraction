package fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HadHospitalStay, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.AlwaysTrueStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.had.HadSimpleExtractor

object HadHospitalStaysExtractor extends HadSimpleExtractor[HospitalStay]
  with AlwaysTrueStrategy[HospitalStay] {
  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = HadHospitalStay

  override def extractValue(row: Row): String = extractGroupId(row)

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def getCodes: SimpleExtractorCodes = SimpleExtractorCodes.empty
}
