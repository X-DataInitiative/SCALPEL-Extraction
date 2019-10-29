// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure}
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

/*
    val exposureN = NLevelInteractionTransformer.elevateExposureN(exposures, 3)

    val down = NLevelInteractionTransformer
      .elevateExposureN(exposures, 3)
      .filter(dataset => dataset.take(1).apply(0).values.size > 1)
      .map(dataset => dataset.flatMap(e => e.toLowerLevelInvolvedExposureN).distinct())

    print(down.zip(exposureN.drop(1)).map(
      e => {
        val down1 = e._1
        val high = e._2

        high.joinWith(down1, high("patientID") === down1("patientID") && high("values") === down1("values"), "left")
      }
    ).map(
      dataset => dataset.groupByKey(e => e._1).flatMapGroups(
        (e, i) =>
          RemainingPeriod.delimitPeriods(
            RightRemainingPeriod(e),
            i.map(_._2).toList.sortBy(_.period.start).map(e => LeftRemainingPeriod[ExposureN](e)),
            List.empty[LeftRemainingPeriod[ExposureN]]
          )
      )
    ).foldLeft(exposureN.head.map(l => l.toInteraction))((acc, b) => acc.union(b.map(l => l.e.toInteraction))).distinct().sort("start").show())*/

    val result = NLevelInteractionTransformer(3).transform(exposures)
    print(result.show())

  }


}
