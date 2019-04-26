package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import fr.polytechnique.cmap.cnam.SharedContext
import org.scalatest.Matchers.{a, convertToAnyShouldWrapper}

class DrugClassficationLevelSuite extends SharedContext{
  "fromString" should "return the correct level given a string" in {

    DrugClassificationLevel.fromString("Therapeutic") shouldBe a[TherapeuticLevel.type]
    DrugClassificationLevel.fromString("Pharmacological") shouldBe a[PharmacologicalLevel.type]
    DrugClassificationLevel.fromString("moleculeCombination") shouldBe a[MoleculeCombinationLevel.type]
    DrugClassificationLevel.fromString("CIP13") shouldBe a[Cip13Level.type]
  }
}
