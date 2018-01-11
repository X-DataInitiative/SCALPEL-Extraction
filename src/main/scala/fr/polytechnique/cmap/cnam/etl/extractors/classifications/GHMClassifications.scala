package fr.polytechnique.cmap.cnam.etl.extractors.classifications

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor


object GHMClassifications extends McoEventRowExtractor{
  def extract(
      mco: DataFrame,
      ghmCodes: Set[String]): Dataset[Event[Classification]] = {

    import mco.sqlContext.implicits._

    if (ghmCodes.isEmpty) {
      return mco.sqlContext.sparkSession.emptyDataset[Event[Classification]]
    }

    val df = prepareDF(mco)
    df.flatMap { r =>
      eventFromRow[Classification](r, GHMClassification, ColNames.GHM, ghmCodes.toSeq)
    }.distinct()
  }
}
