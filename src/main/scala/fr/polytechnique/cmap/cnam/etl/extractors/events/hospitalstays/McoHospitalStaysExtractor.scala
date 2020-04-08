// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays

import java.sql.{Date, Timestamp}
import scala.util.Try
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, McoHospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.AlwaysTrueStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoSimpleExtractor

object McoHospitalStaysExtractor extends McoSimpleExtractor[HospitalStay] with AlwaysTrueStrategy[HospitalStay] {

  override def getCodes: SimpleExtractorCodes = SimpleExtractorCodes.empty

  override def columnName: String = ColNames.EndDate

  override def eventBuilder: EventBuilder = McoHospitalStay

  override def neededColumns: List[String] = List(ColNames.StayFrom, ColNames.StayFromType) ++ super.usedColumns

  override def extractEnd(r: Row): Option[Timestamp] = Some(new Timestamp(r.getAs[Date](ColNames.EndDate).getTime))

  override def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

  override def extractValue(row: Row): String = extractGroupId(row)

  override def extractWeight(r: Row): Double = {
    getFromValue(r).flatMap(from => getFromType(r).map(fromType => from + fromType * 0.1)) recover { case _ => -1D }
  }.get

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
}
