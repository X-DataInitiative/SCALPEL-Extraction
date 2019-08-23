package fr.polytechnique.cmap.cnam.etl.extractors.acts

import java.sql.Timestamp
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits.DateImplicits
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

object McoCcamActExtractor extends McoExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = McoCCAMAct

  override def extractStart(r: Row): Timestamp = {
    (DateImplicits(r.getAs[Timestamp](NewColumns.EstimatedStayStart)) + Period(days = getDateOffset(r))).get
  }

  def getDateOffset(r: Row): Int = r.getAs[String](ColNames.CCAMDelayDate) match {
    case null => 0
    case value: String => value.toInt
  }
}

object McoCimMedicalActExtractor extends McoExtractor[MedicalAct] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = McoCIM10Act
}