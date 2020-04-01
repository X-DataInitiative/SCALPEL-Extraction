package fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, SsrHospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.AlwaysTrueStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.ssr.SsrSimpleExtractor

object SsrHospitalStaysExtractor extends SsrSimpleExtractor[HospitalStay] with AlwaysTrueStrategy[HospitalStay] {
  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = SsrHospitalStay

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

  override def extractValue(row: Row): String = extractGroupId(row)

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RhaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  override def getCodes: SimpleExtractorCodes = SimpleExtractorCodes.empty
}
