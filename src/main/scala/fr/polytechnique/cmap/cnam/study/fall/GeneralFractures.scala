package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

object GeneralFractures extends OutcomeTransformer with FractureCodes {

  override val outcomeName: String = "generic_fall"

  def transform(
      diagnoses: Dataset[Event[Diagnosis]],
      classifications: Dataset[Event[Classification]],
      medicalActs: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {

    import diagnoses.sqlContext.implicits._
    unionDatasets(
      HospitalizedFractures.transform(diagnoses, classifications),
      PrivateAmbulatoryFractures.transform(medicalActs),
      PublicAmbulatoryFractures.transform(medicalActs)
    ).map(_.copy(value = outcomeName))
  }
}
