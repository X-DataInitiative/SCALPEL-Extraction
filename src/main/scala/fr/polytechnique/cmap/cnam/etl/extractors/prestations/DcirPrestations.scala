package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirEventRowExtractor

private[prestations] case class DcirPrestations(
    medicalSpeCodes: Seq[String],
    nonMedicalSpeCodes: Seq[String]) extends DcirEventRowExtractor {

  override def extractors: List[DcirRowExtractor] = List(
    DcirRowExtractor(ColNames.MSpe, medicalSpeCodes, MedicalPrestation),
    DcirRowExtractor(ColNames.NonMSpe, nonMedicalSpeCodes, NonMedicalPrestation)
  )

  override def extractorCols: List[String] = List(ColNames.MSpe, ColNames.NonMSpe)

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.ExecPSNum)
  }

  def extract(dcir: DataFrame): Dataset[Event[PrestationSpeciality]] =
    super.extract[PrestationSpeciality](dcir)
}
