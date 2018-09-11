package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor

private[diagnoses] case class McoDiagnoses(
  dpCodes: Seq[String],
  drCodes: Seq[String],
  daCodes: Seq[String]) extends McoEventRowExtractor {

  override def extractors: List[McoRowExtractor] = List(
    McoRowExtractor(ColNames.DP, dpCodes, MainDiagnosis),
    McoRowExtractor(ColNames.DR, drCodes, LinkedDiagnosis),
    McoRowExtractor(ColNames.DA, daCodes, AssociatedDiagnosis)
  )

  override def extractorCols: List[String] = List(ColNames.DA, ColNames.DP, ColNames.DR)

  def extract(
    mco: DataFrame): Dataset[Event[Diagnosis]] = super.extract[Diagnosis](mco)
}
