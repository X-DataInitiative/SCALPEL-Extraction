package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.fall.FracturesTransformerImplicits._
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes
import fr.polytechnique.cmap.cnam.study.fall.config.{FallConfig, FracturesTransformerConfig}
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

class FracturesTransformer(config: FallConfig) extends OutcomesTransformer with FractureCodes {

  override val outcomeName: String = "all_fall"

    def transform(
       liberalActs: Dataset[Event[MedicalAct]],
       acts: Dataset[Event[MedicalAct]],
       diagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {

      // Hospitalized fractures
      val hospitralizedFractures = HospitalizedFractures.transform(diagnoses, acts, config.sites.sites)

      // Liberal Fractures
      val liberalFractures = LiberalFractures.transform(liberalActs)

      // Public Ambulatory Fractures
      val publicAmbulatoryFractures = PublicAmbulatoryFractures.transform(acts)

      // Private Ambulatory Fractures
      val privateAmbulatoryFractures = PrivateAmbulatoryFractures.transform(acts)

      unionDatasets(
        hospitralizedFractures,
        liberalFractures,
        publicAmbulatoryFractures,
        privateAmbulatoryFractures
      ).groupConsecutiveFractures(config.outcomes.fallFrame)
    }

}


