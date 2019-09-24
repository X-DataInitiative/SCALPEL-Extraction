package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.{Date, Timestamp}

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, HadHospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

object HadHospitalStaysExtractor extends HadExtractor[HospitalStay] {
  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = HadHospitalStay

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

  override def isInStudy(codes: Set[String])(row: Row): Boolean = true

  override def code: Row => String = extractGroupId

  override def getInput(sources: Sources): DataFrame = sources.had.get.select(ColNames.hospitalStayPart.map(col): _*)

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNumEpmsi) + "_" +
      r.getAs[String](ColNames.RhadNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }
}
