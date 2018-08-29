package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.{DataFrame, Dataset, functions}
import fr.polytechnique.cmap.cnam.etl.events.{MedicalAct, _}
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoEventRowExtractor

private[acts] case class McoMedicalActs(cimCodes: Seq[String], ccamCodes: Seq[String]) extends McoEventRowExtractor {

  override def extractorCols: List[String] = List(ColNames.DP, ColNames.CCAM)

  def extract(
    mco: DataFrame,
    cimCodes: Seq[String],
    ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {

    import mco.sqlContext.implicits._

    mco.estimateStayStartTime
      .select(inputCols.map(functions.col): _*)
      .flatMap { r =>
        lazy val patientId = extractPatientId(r)
        lazy val groupId = extractGroupId(r)
        lazy val eventDate = extractStart(r)

        eventFromRow[MedicalAct](r, McoCIM10Act, ColNames.DP, cimCodes) ++
          eventFromRow[MedicalAct](r, McoCCAMAct, ColNames.CCAM, ccamCodes)
      }.distinct
  }

  override def extractors: List[McoRowExtractor] = List(McoRowExtractor(ColNames.DP, cimCodes, McoCIM10Act),
    McoRowExtractor(ColNames.CCAM, ccamCodes, McoCCAMAct))

  def extract(
    mco: DataFrame): Dataset[Event[MedicalAct]] = super.extract[MedicalAct](mco)
}