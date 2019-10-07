// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.scalatest.Matchers.{a, convertToAnyShouldWrapper}
import fr.polytechnique.cmap.cnam.SharedContext

class MedicalActsConfigSuite extends SharedContext {
  "MedicalActsConfig" should "be of type MedicalActsConfig" in {
    MedicalActsConfig.apply() shouldBe a[MedicalActsConfig]
  }
}
