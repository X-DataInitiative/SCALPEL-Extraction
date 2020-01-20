// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.time.LocalDate
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions._

class ObservationPeriodTransformerSuite extends SharedContext {

  val testConfig = new ObservationPeriodTransformerConfig(
    studyStart = LocalDate.of(2006, 1, 1),
    studyEnd = LocalDate.of(2010, 1, 1)
  )

  "transform" should "return a Dataset[Event[ObservationPeriod]] with the observation events of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val events: Dataset[Event[AnyEvent]] = Seq[Event[AnyEvent]](
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008, 1, 20)),
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008, 1, 1)),
      Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008, 1, 10)),
      Outcome("Patient_A", "C67", makeTS(2007, 1, 1)),
      Molecule("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2009, 1, 1)),
      Outcome("Patient_B", "C67", makeTS(2007, 1, 1))
    ).toDS

    val expected: Dataset[Event[ObservationPeriod]] = Seq(
      ObservationPeriod("Patient_A", makeTS(2008, 1, 1), makeTS(2010, 1, 1)),
      ObservationPeriod("Patient_B", makeTS(2009, 1, 1), makeTS(2010, 1, 1))
    ).toDS

    val transformer: ObservationPeriodTransformer = new ObservationPeriodTransformer(testConfig)

    // When
    val result: Dataset[Event[ObservationPeriod]] = transformer.transform(events)


    // Then
    assertDSs(result, expected)
  }
}
