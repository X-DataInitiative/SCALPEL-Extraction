package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor


case class GHMClassifications(ghmCodes: Seq[String]) extends McoEventRowExtractor {
  def extract(
    mco: DataFrame,
    ghmCodes: Set[String]): Dataset[Event[Classification]] = {

    import mco.sqlContext.implicits._

    if (ghmCodes.isEmpty) {
      return mco.sqlContext.sparkSession.emptyDataset[Event[Classification]]
    }

    val df = estimateStayStartTime(mco)
    df.flatMap { r =>
      eventFromRow[Classification](r, GHMClassification, ColNames.GHM, ghmCodes.toSeq)
    }.distinct()
  }

  override def extractors: List[McoRowExtractor] = List(McoRowExtractor(ColNames.GHM, ghmCodes, GHMClassification))

  override def extractorCols: List[String] = List(ColNames.GHM)

  def extract(
    mco: DataFrame): Dataset[Event[Classification]] = super
    .extract[Classification](mco)
}
