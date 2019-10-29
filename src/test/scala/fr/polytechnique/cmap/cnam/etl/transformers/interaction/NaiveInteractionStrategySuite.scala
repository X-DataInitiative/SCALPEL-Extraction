// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class NaiveInteractionStrategySuite extends SharedContext {
  "transform" should "create a DataSet of Level1 & Level2 Interaction" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val exposures: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 3, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
    ).toDS()

    val expected: Dataset[Event[Interaction]] = Seq[Event[Interaction]](
      Interaction("Federer", "King", "Alpeazolam_Dopamine", 2.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 3, 1))),
      Interaction("Federer", "King", "Dopamine_Paracetamol", 2.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 5, 1))),
      Interaction("Federer", "King", "Dopamine_Paracetamol", 2.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Interaction("Federer", "King", "Diazepam_Dopamine", 2.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 5, 1))),
      Interaction("Federer", "King", "Diazepam_Paracetamol", 2.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1))),
      Interaction("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 6, 1), Some(makeTS(2019, 7, 1))),
      Interaction("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 2, 1)))
    ).toDS

    val result = NaiveInteractionStrategy.transform(exposures)

    assertDSs(result, expected)
  }


  "elevateExposure" should "join and return a DataSet of Exposure tuple" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val exposures: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 3, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
    ).toDS()

    val expected: Dataset[(Event[Exposure], Event[Exposure])] = Seq[(Event[Exposure], Event[Exposure])](
      (
        Exposure("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 3, 1))),
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
        Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
        Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
      )
    ).toDS

    val result = NaiveInteractionStrategy.elevateExposure(exposures)

    assertDSs(result, expected)
  }

  "fromTupleExposureToSecondLevelInteraction" should "combine the Exposure tuple and create an Interaction of Level 2" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input: Dataset[(Event[Exposure], Event[Exposure])] = Seq[(Event[Exposure], Event[Exposure])](
      (
        Exposure("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 3, 1))),
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
        Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
        Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
      )
    ).toDS

    val expected: Dataset[Event[Interaction]] = Seq[Event[Interaction]](
      Interaction("Federer", "King", "Alpeazolam_Dopamine", 2.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 3, 1))),
      Interaction("Federer", "King", "Dopamine_Paracetamol", 2.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 5, 1))),
      Interaction("Federer", "King", "Dopamine_Paracetamol", 2.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Interaction("Federer", "King", "Diazepam_Dopamine", 2.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 5, 1))),
      Interaction("Federer", "King", "Diazepam_Paracetamol", 2.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
    ).toDS

    val result = input.transform(NaiveInteractionStrategy.fromElevatedExposuresToSecondLevelInteractions)

    assertDSs(result, expected)
  }

  "findInteractionPeriod" should "find for each value a period in which it is involved in an Interaction" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input: Dataset[(Event[Exposure], Event[Exposure])] = Seq[(Event[Exposure], Event[Exposure])](
      (
        Exposure("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 3, 1))),
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
        Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
      )
      ,
      (
        Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
        Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
      )
    ).toDS

    val expected: Dataset[InterActionPeriod] = Seq[InterActionPeriod](
      InterActionPeriod("Federer", "Dopamine", makeTS(2019, 2, 1), makeTS(2019, 3, 1)),
      InterActionPeriod("Federer", "Alpeazolam",makeTS(2019, 2, 1), makeTS(2019, 3, 1)),
      InterActionPeriod("Federer", "Dopamine",makeTS(2019, 3, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 3, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Dopamine",makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Diazepam",makeTS(2019, 4, 1), makeTS(2019, 6, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 4, 1), makeTS(2019, 6, 1)),
      InterActionPeriod("Federer", "Diazepam",makeTS(2019, 4, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Dopamine",makeTS(2019, 4, 1), makeTS(2019, 5, 1))
    ).toDS

    val result = input.transform(NaiveInteractionStrategy.findInteractionPeriods)

    assertDSs(result, expected)
  }

  "findLongestUninterruptedInteractionPeriods" should "find for each value the longest uninterrupted interaction periods where it is involved" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input: Dataset[InterActionPeriod] = Seq[InterActionPeriod](
      InterActionPeriod("Federer", "Dopamine", makeTS(2019, 2, 1), makeTS(2019, 3, 1)),
      InterActionPeriod("Federer", "Alpeazolam",makeTS(2019, 2, 1), makeTS(2019, 3, 1)),
      InterActionPeriod("Federer", "Dopamine",makeTS(2019, 3, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 3, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Dopamine",makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Diazepam",makeTS(2019, 4, 1), makeTS(2019, 6, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 4, 1), makeTS(2019, 6, 1)),
      InterActionPeriod("Federer", "Diazepam",makeTS(2019, 4, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Dopamine",makeTS(2019, 4, 1), makeTS(2019, 5, 1))
    ).toDS

    val expected: Dataset[InterActionPeriod] = Seq[InterActionPeriod](
      InterActionPeriod("Federer", "Dopamine", makeTS(2019, 2, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Dopamine", makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Alpeazolam",makeTS(2019, 2, 1), makeTS(2019, 3, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 3, 1), makeTS(2019, 6, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Diazepam",makeTS(2019, 4, 1), makeTS(2019, 6, 1))
    ).toDS

    val result = input.transform(NaiveInteractionStrategy.findLongestUninterruptedInteractionPeriods)

    assertDSs(result, expected)
  }

  "elevateToLevel1Interaction" should "should create interaction where there is one molecule active" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val exposures: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Nadal", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 9, 1))),
      Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 9, 1))),
      Exposure("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2019, 3, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
    ).toDS()

    val interactionPeriods: Dataset[InterActionPeriod] = Seq[InterActionPeriod](
      InterActionPeriod("Federer", "Dopamine", makeTS(2019, 2, 1), makeTS(2019, 5, 1)),
      InterActionPeriod("Federer", "Dopamine", makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Alpeazolam",makeTS(2019, 2, 1), makeTS(2019, 3, 1)),
      InterActionPeriod("Federer", "Alpeazolam",makeTS(2018, 6, 1), makeTS(2018, 7, 1)),
      InterActionPeriod("Federer", "Alpeazolam",makeTS(2018, 10, 1), makeTS(2018, 11, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 3, 1), makeTS(2019, 6, 1)),
      InterActionPeriod("Federer", "Paracetamol",makeTS(2019, 7, 1), makeTS(2019, 8, 1)),
      InterActionPeriod("Federer", "Diazepam",makeTS(2019, 4, 1), makeTS(2019, 6, 1))
    ).toDS

    val expected: Dataset[Event[Interaction]] = Seq[Event[Interaction]](
      Interaction("Nadal", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 9, 1))),
      Interaction("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 6, 1), Some(makeTS(2019, 7, 1))),
      Interaction("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 8, 1), Some(makeTS(2019, 9, 1))),
      Interaction("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 3, 1), Some(makeTS(2018, 6, 1))),
      Interaction("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 7, 1), Some(makeTS(2018, 10, 1))),
      Interaction("Federer", "King", "Alpeazolam", 1.0D, makeTS(2018, 11, 1), Some(makeTS(2019, 2, 1)))
    ).toDS()

    val result = NaiveInteractionStrategy.elevateToLevel1Interaction(interactionPeriods)(exposures)

    assertDSs(result, expected)
  }

}
