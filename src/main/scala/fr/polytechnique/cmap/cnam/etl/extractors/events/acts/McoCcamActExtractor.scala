// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import java.sql.Timestamp
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits.DateImplicits
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, McoCCAMAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoSimpleExtractor


final case class McoCcamActExtractor(codes: SimpleExtractorCodes) extends McoSimpleExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {
  override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = McoCCAMAct

  override def usedColumns: List[String] = ColNames.CCAMDelayDate :: super.usedColumns

  override def getCodes: SimpleExtractorCodes = codes

  override def extractStart(r: Row): Timestamp = {
    (r.getAs[Timestamp](NewColumns.EstimatedStayStart) + Period(days = getDateOffset(r))).get
  }

  def getDateOffset(r: Row): Int = r.getAs[String](ColNames.CCAMDelayDate) match {
    case null => 0
    case value: String => value.toInt
  }
}
