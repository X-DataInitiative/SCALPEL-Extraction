package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.NewMcoExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object NewHospitalStaysExtractor extends NewMcoExtractor[HospitalStay]{
  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = HospitalStay

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

  override def isInStudy(codes: Set[String])(row: Row): Boolean = true
  override def code: Row => String = extractGroupId

  override def getInput(sources: Sources): DataFrame = sources.mco.get
}
