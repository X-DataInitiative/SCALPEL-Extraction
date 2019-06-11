package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor

@deprecated("I said so")
private[diagnoses] case class McoDiagnoses(
  dpCodes: Seq[String],
  drCodes: Seq[String],
  daCodes: Seq[String]) extends McoEventRowExtractor {

  override def extractors: List[McoRowExtractor] = List(
    McoRowExtractor(ColNames.DP, dpCodes, MainDiagnosis.category),
    McoRowExtractor(ColNames.DR, drCodes, LinkedDiagnosis.category),
    McoRowExtractor(ColNames.DA, daCodes, AssociatedDiagnosis.category)
  )

  override def extractorCols: List[String] = List(ColNames.DA, ColNames.DP, ColNames.DR)

  def extract(
    mco: DataFrame): Dataset[Event[Diagnosis]] = super.extract[Diagnosis](mco)
}
