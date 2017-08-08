package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor

private[acts] object McoMedicalActs extends McoEventRowExtractor {

  def extract(
      mco: DataFrame,
      cimCodes: Seq[String],
      ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {

    import mco.sqlContext.implicits._
    val df = prepareDF(mco)
    df.flatMap { r =>
      eventFromRow[MedicalAct](r, McoCIM10Act, ColNames.DP, cimCodes) ++
      eventFromRow[MedicalAct](r, McoCCAMAct, ColNames.CCAM, ccamCodes)
    }.distinct
  }
}
