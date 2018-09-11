package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor


case class GHMClassifications(ghmCodes: Seq[String]) extends McoEventRowExtractor {

  override def extractors: List[McoRowExtractor] = List(McoRowExtractor(ColNames.GHM, ghmCodes, GHMClassification))

  override def extractorCols: List[String] = List(ColNames.GHM)

  def extract(
    mco: DataFrame): Dataset[Event[Classification]] = super
    .extract[Classification](mco)
}
