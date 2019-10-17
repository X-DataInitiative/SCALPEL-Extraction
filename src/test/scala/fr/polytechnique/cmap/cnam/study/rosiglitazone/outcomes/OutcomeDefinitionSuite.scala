// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import org.scalatest.FlatSpec

class OutcomeDefinitionSuite extends FlatSpec {

  "types" should "list all options" in {
    assert(
      OutcomeDefinition.default.types.exists {
        case OutcomeDefinition.Infarctus => true
        case OutcomeDefinition.HeartFailure => true
      }
    )
  }

  "default" should "return Infarctus" in {
    assert(OutcomeDefinition.default == OutcomeDefinition.Infarctus)
  }

  "Infarctus" should "have the correct outcome name" in {
    assert(OutcomeDefinition.Infarctus.outcomeName == "infarctus")
  }

  "HeartFailure" should "have the correct outcome name" in {
    assert(OutcomeDefinition.HeartFailure.outcomeName == "heart_failure")
  }
}
