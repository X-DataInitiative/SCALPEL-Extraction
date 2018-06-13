package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor

private[diagnoses] object McoDiagnoses extends McoEventRowExtractor {

  def extract(
      mco: DataFrame,
      dpCodes: Seq[String],
      drCodes: Seq[String],
      daCodes: Seq[String]): Dataset[Event[Diagnosis]] = {

    import mco.sqlContext.implicits._
    mco.flatMap { r =>
      eventFromRow[Diagnosis](r, MainDiagnosis, ColNames.DP, dpCodes) ++
      eventFromRow[Diagnosis](r, LinkedDiagnosis, ColNames.DR, drCodes) ++
      eventFromRow[Diagnosis](r, AssociatedDiagnosis, ColNames.DA, daCodes)
    }.distinct
  }
}
