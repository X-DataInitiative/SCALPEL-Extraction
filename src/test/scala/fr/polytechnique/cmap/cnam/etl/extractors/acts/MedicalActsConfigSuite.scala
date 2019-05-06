package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.SharedContext
import org.scalatest.Matchers.{a, convertToAnyShouldWrapper}

class MedicalActsConfigSuite extends SharedContext {
  "MedicalActsConfig" should "be of type MedicalActsConfig" in {
    MedicalActsConfig.apply() shouldBe a[MedicalActsConfig]
  }
}
