package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.{Date, Timestamp}

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, SsrHospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.ssr.SsrExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

object SsrHospitalStaysExtractor extends SsrExtractor[HospitalStay] {
  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = SsrHospitalStay

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

  override def isInStudy(codes: Set[String])(row: Row): Boolean = true

  override def code: Row => String = extractGroupId

  override def getInput(sources: Sources): DataFrame = sources.ssr.get.select(ColNames.hospitalStayPart.map(col): _*)

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RhaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }
}
