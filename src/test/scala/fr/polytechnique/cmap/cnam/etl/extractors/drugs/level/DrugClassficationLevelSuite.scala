// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import org.scalatest.Matchers.{a, convertToAnyShouldWrapper}
import fr.polytechnique.cmap.cnam.SharedContext

class DrugClassficationLevelSuite extends SharedContext {
  "fromString" should "return the correct level given a string" in {

    DrugClassificationLevel.fromString("Therapeutic") shouldBe a[TherapeuticLevel.type]
    DrugClassificationLevel.fromString("Pharmacological") shouldBe a[PharmacologicalLevel.type]
    DrugClassificationLevel.fromString("moleculeCombination") shouldBe a[MoleculeCombinationLevel.type]
    DrugClassificationLevel.fromString("CIP13") shouldBe a[Cip13Level.type]
  }
}
