// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class NLevelInteractionTransformerSuite extends SharedContext {

  "elevateExposureN" should "join and return a DataSet of ExposureN" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val exposures: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Alprazolam", 1.0D, makeTS(2019, 1, 1), Some(makeTS(2019, 3, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
    ).toDS()

    val expected: List[Dataset[ExposureN]] = List(
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol", "Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Alprazolam", "Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 3, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 1, 1), makeTS(2019, 3, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1)))
      ).toDS()
    )

    val result = NLevelInteractionTransformer(3).elevateToExposureN(exposures, 3)
    // The mapping is necessary for now as Spark seems to struggle with nested Data Structures
    result.zip(expected).foreach(e => assertDSs(e._1.map(_.toInteraction).distinct(), e._2.distinct().map(_.toInteraction).distinct()))

  }

  "trickleDownExposureN" should "give the involvement of ExposureN level n to level n-1" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given

    val input: List[Dataset[ExposureN]] = List(
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol", "Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Alprazolam", "Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 3, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 1, 1), makeTS(2019, 3, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1)))
      ).toDS()
    )

    val expected: List[Dataset[ExposureN]] = List(
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),

        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),

        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),

        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),

        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1)))
      ).toDS()
    )

    val result = NLevelInteractionTransformer(3).trickleDownExposureN(input)
    // The mapping is necessary for now as Spark seems to struggle with nested Data Structures
    result.zip(expected).foreach(e => assertDSs(e._1.map(_.toInteraction).distinct(), e._2.distinct().map(_.toInteraction).distinct()))
  }

  "reduceHigherExposuresNFromLowerExposures" should "reduce the time period of higher ExposureN from lower ExposureN" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val interactions: List[Dataset[ExposureN]] = List(
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Alprazolam", "Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 3, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 1, 1), makeTS(2019, 3, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1)))
      ).toDS()
    )

    val higherInteractionInvolvement: List[Dataset[ExposureN]] = List(
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 3, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1))),
        ExposureN("Federer", Set("Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1)))
      ).toDS()
    )

    val expected: List[Dataset[ExposureN]] = List(
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 4, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 5, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Alprazolam", "Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1)))
      ).toDS(),
      Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 6, 1), makeTS(2019, 7, 1))),
        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 1, 1), makeTS(2019, 2, 1)))
      ).toDS()
    )

    val result = NLevelInteractionTransformer(3).reduceHigherExposuresNFromLowerExposures(interactions, higherInteractionInvolvement)
    result.zip(expected).foreach(e => assertDSs(e._1.map(_.e.toInteraction).distinct(), e._2.distinct().map(_.toInteraction).distinct()))
  }

  "transform" should "create interactions of level N" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val exposures: Dataset[Event[Exposure]] = Seq[Event[Exposure]](
      Exposure("Federer", "King", "Paracetamol", 1.0D, makeTS(2019, 3, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Alprazolam", 1.0D, makeTS(2019, 1, 1), Some(makeTS(2019, 3, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 2, 1), Some(makeTS(2019, 5, 1))),
      Exposure("Federer", "King", "Dopamine", 1.0D, makeTS(2019, 7, 1), Some(makeTS(2019, 8, 1))),
      Exposure("Federer", "King", "Diazepam", 1.0D, makeTS(2019, 4, 1), Some(makeTS(2019, 6, 1)))
    ).toDS()

    val expected: Dataset[Event[Interaction]] = Seq[ExposureN](
        ExposureN("Federer", Set("Paracetamol", "Dopamine", "Diazepam"), Period(makeTS(2019, 4, 1), makeTS(2019, 5, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 3, 1), makeTS(2019, 4, 1))),
        ExposureN("Federer", Set("Paracetamol", "Dopamine"), Period(makeTS(2019, 7, 1), makeTS(2019, 8, 1))),
        ExposureN("Federer", Set("Paracetamol", "Diazepam"), Period(makeTS(2019, 5, 1), makeTS(2019, 6, 1))),
        ExposureN("Federer", Set("Alprazolam", "Dopamine"), Period(makeTS(2019, 2, 1), makeTS(2019, 3, 1))),
        ExposureN("Federer", Set("Paracetamol"), Period(makeTS(2019, 6, 1), makeTS(2019, 7, 1))),
        ExposureN("Federer", Set("Alprazolam"), Period(makeTS(2019, 1, 1), makeTS(2019, 2, 1)))
      ).toDS.map[Event[Interaction]]((e: ExposureN) => e.toInteraction)

    val result = NLevelInteractionTransformer(6).transform(exposures)

    assertDSs(result, expected, true)
  }

}
