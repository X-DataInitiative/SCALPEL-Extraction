package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import org.scalatest.Matchers.convertToAnyShouldWrapper
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class FollowUpSuite extends SharedContext {

  "isValid" should "return true for FollowUps where start < stop, false otherwise" in {

    //Given
    val followUpPeriods = Seq(
      FollowUp("Patient_A", makeTS(2006, 6, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_B", makeTS(2006, 7, 1), makeTS(2006, 7, 1), "any_reason"),
      FollowUp("Patient_C", makeTS(2016, 8, 1), makeTS(2009, 12, 31), "any_reason")
    )

    val expected = Seq(
      FollowUp("Patient_A", makeTS(2006, 6, 1), makeTS(2009, 12, 31), "any_reason")
    )

    val result = followUpPeriods.filter(_.isValid)

    result shouldEqual expected
  }
}
