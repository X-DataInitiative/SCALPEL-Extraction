package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events.{MedicalAct, _}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor

private[acts] case class McoMedicalActs(cimCodes: Seq[String], ccamCodes: Seq[String]) extends McoEventRowExtractor {

  override def extractorCols: List[String] = List(ColNames.DP, ColNames.CCAM)

  override def extractors: List[McoRowExtractor] = List(
    McoRowExtractor(ColNames.DP, cimCodes, McoCIM10Act),
    McoRowExtractor(ColNames.CCAM, ccamCodes, McoCCAMAct)
  )

  def extract(mco: DataFrame): Dataset[Event[MedicalAct]] = super.extract[MedicalAct](mco)
}