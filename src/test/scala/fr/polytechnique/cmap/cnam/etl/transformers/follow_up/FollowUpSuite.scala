// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, FollowUp}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class FollowUpSuite extends SharedContext {

  "isValid" should "return true for FollowUps where start < stop, false otherwise" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val followUpPeriods: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2009, 12, 31)),
      FollowUp("Patient_B", "any_reason", makeTS(2006, 7, 1), makeTS(2006, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2016, 8, 1), makeTS(2009, 12, 31))
    ).toDS

    val expected: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2009, 12, 31))
    ).toDS

    // When
    val result: Dataset[Event[FollowUp]] = followUpPeriods.filter(e => e.start.before(e.end.get))

    //Then
    assertDSs(result, expected)
  }
}
