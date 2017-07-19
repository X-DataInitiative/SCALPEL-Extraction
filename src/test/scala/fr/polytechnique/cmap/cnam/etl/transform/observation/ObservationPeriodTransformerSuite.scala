package fr.polytechnique.cmap.cnam.etl.transform.observation

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions._

class ObservationPeriodTransformerSuite extends SharedContext {

    "withObservationStart" should "add a column with the start of the observation period" in {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      // Given
      val input = Seq(
        ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 20)),
        ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1)),
        ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 10)),
        ("Patient_A", "disease", "Hello World!", makeTS(2007, 1, 1)),
        ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1)),
        ("Patient_B", "disease", "Hello World!", makeTS(2007, 1, 1))
      ).toDF("patientID", "category", "eventId", "start")

      val expected = Seq(
        ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 20), makeTS(2008, 1, 1)),
        ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 1), makeTS(2008, 1, 1)),
        ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 1, 10), makeTS(2008, 1, 1)),
        ("Patient_A", "disease", "Hello World!", makeTS(2007, 1, 1), makeTS(2008, 1, 1)),
        ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), makeTS(2009, 1, 1)),
        ("Patient_B", "disease", "Hello World!", makeTS(2007, 1, 1), makeTS(2009, 1, 1))
      ).toDF("patientID", "category", "eventId", "start", "observationStart")

      val transformer = new ObservationPeriodTransformer(
        makeTS(2006, 1, 1),
        makeTS(2009, 12, 31, 23, 59, 59))

      // When
      import transformer._
      val result = input.withObservationStart

      // Then
      assertDFs(result, expected)
    }

    "transform" should "return a Dataset[FlatEvent] with the observation events of each patient" in {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      // Given
      val events = Seq[Event[AnyEvent]](
        Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008, 1, 20)),
        Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008, 1, 1)),
        Molecule("Patient_A", "PIOGLITAZONE", 1.0, makeTS(2008, 1, 10)),
        Outcome("Patient_A", "C67", makeTS(2007, 1, 1)),
        Molecule("Patient_B", "PIOGLITAZONE", 1.0, makeTS(2009, 1, 1)),
        Outcome("Patient_B", "C67", makeTS(2007, 1, 1))
      ).toDS

      val expected = Seq(
        ObservationPeriod("Patient_A", makeTS(2008, 1, 1), makeTS(2009, 12, 31, 23, 59, 59)),
        ObservationPeriod("Patient_B", makeTS(2009, 1, 1), makeTS(2009, 12, 31, 23, 59, 59))
      ).toDS

      val transformer = new ObservationPeriodTransformer(
        makeTS(2006, 1, 1),
        makeTS(2009, 12, 31, 23, 59, 59))

      // When
      val result = transformer.transform(events)

      // Then
      assertDSs(result, expected)
    }

}
