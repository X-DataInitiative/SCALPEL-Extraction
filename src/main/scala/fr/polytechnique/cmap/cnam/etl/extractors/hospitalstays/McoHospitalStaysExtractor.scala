package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.{Date, Timestamp}
import scala.util.Try
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, McoHospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object McoHospitalStaysExtractor extends McoExtractor[HospitalStay] {
  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = McoHospitalStay

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

  override def isInStudy(codes: Set[String])(row: Row): Boolean = true

  override def code: Row => String = extractGroupId

  override def extractWeight(r: Row): Double = {
    getFromValue(r) flatMap (from => getFromType(r) map (fromType => from + fromType * 0.1)) recover { case _ => -1D } get
  }

  private def getFromValue(r: Row): Try[Double] = {
    Try {
      r.getAs[String](ColNames.StayFrom).toDouble
    }
  }

  private def getFromType(r: Row): Try[Double] = {

    val isNull = (s: String) => s == null || s.trim.isEmpty

    Try {
      r.getAs[String](ColNames.StayFromType) match {
        case value if isNull(value) => 0D
        case "R" => 8D
        case value => value.toDouble
      }
    }
  }

  override def getInput(sources: Sources): DataFrame = sources.mco.get.select(ColNames.hospitalStayPart.map(col): _*)
}
