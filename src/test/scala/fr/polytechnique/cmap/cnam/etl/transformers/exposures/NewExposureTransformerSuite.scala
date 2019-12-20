// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.Dataset
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event, FollowUp}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class NewExposureTransformerSuite extends SharedContext{
  "toExposure" should "tranform drugs to exposure based on parameters" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given

    val input: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "Antidepresseurs", 1, makeTS(2014, 6, 8)),
      Drug("patient4", "Antidepresseurs", 1, makeTS(2014, 8, 1)),
      Drug("patient", "Antidepresseurs", 1, makeTS(2014, 2, 5)),Drug("patient8", "Antidepresseurs", 1, makeTS(2014, 9, 1))
    ).toDS
    val followUp: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2009, 12, 31)),
      FollowUp("Patient_B", "any_reason", makeTS(2006, 7, 1), makeTS(2006, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2016, 8, 1), makeTS(2009, 12, 31))

    ).toDS()


    val exposureTransformer = new NewExposureTransformer(new NewExposuresTransformerConfig(NLimitedExposureAdder(0.days, 15.days, 90.days, 30.days)))

    val result = exposureTransformer.transform(followUp)(input)
    print(result.show())
  }
}

