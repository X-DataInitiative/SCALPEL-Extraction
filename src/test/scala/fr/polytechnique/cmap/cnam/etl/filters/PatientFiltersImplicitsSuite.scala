// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.filters

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Molecule, Outcome}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PatientFiltersImplicitsSuite extends SharedContext {

  "filterEarlyDiagnosedPatients" should "keep only patients who have an outcome after the begining of their followup" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients: Dataset[Patient] = Seq(
      Patient("Patient_A", 0, makeTS(1950, 1, 1), None),
      Patient("Patient_B", 0, makeTS(1940, 1, 1), Some(makeTS(2007, 1, 1))),
      Patient("Patient_C", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_D", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_E", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_F", 0, makeTS(1945, 1, 1), None)
    ).toDS

    val outcomes: Dataset[Event[Outcome]] = Seq(
      Outcome("Patient_A", "outcome_a", makeTS(2006, 5, 1)),
      Outcome("Patient_B", "outcome_a", makeTS(2006, 6, 1)),
      Outcome("Patient_B", "outcome_a", makeTS(2006, 7, 1)),
      Outcome("Patient_B", "outcome_b", makeTS(2006, 8, 1)),
      Outcome("Patient_C", "outcome_b", makeTS(2006, 6, 1)),
      Outcome("Patient_C", "outcome_a", makeTS(2006, 9, 1)),
      Outcome("Patient_C", "outcome_b", makeTS(2006, 10, 1)),
      Outcome("Patient_E", "outcome_a", makeTS(2006, 5, 1))
    ).toDS

    val followUpPeriods = Seq(
      FollowUp("Patient_A", makeTS(2006, 6, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_B", makeTS(2006, 7, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_C", makeTS(2006, 8, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_E", makeTS(2006, 8, 1), makeTS(2009, 12, 31), "any_reason")
    ).toDS

    val expected = Seq(
      Patient("Patient_C", 0, makeTS(1945, 1, 1), None)
    ).toDS

    // When
    val instance = new PatientFiltersImplicits(patients)
    val result = instance.filterEarlyDiagnosedPatients(outcomes, followUpPeriods, "outcome_a")

    // Then
    assertDSs(result, expected)
  }

  "removeEarlyDiagnosedPatients" should "drop who had an outcome before follow-up start" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients: Dataset[Patient] = Seq(
      Patient("Patient_A", 0, makeTS(1950, 1, 1), None),
      Patient("Patient_B", 0, makeTS(1940, 1, 1), Some(makeTS(2007, 1, 1))),
      Patient("Patient_C", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_D", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_E", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_F", 0, makeTS(1945, 1, 1), None)
    ).toDS

    val outcomes: Dataset[Event[Outcome]] = Seq(
      Outcome("Patient_A", "outcome_a", makeTS(2006, 5, 1)),
      Outcome("Patient_B", "outcome_a", makeTS(2006, 6, 1)),
      Outcome("Patient_B", "outcome_a", makeTS(2006, 7, 1)),
      Outcome("Patient_B", "outcome_b", makeTS(2006, 8, 1)),
      Outcome("Patient_C", "outcome_b", makeTS(2006, 6, 1)),
      Outcome("Patient_C", "outcome_a", makeTS(2006, 9, 1)),
      Outcome("Patient_C", "outcome_b", makeTS(2006, 10, 1)),
      Outcome("Patient_E", "outcome_a", makeTS(2006, 5, 1))
    ).toDS

    val followUpPeriods = Seq(
      FollowUp("Patient_A", makeTS(2006, 6, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_B", makeTS(2006, 7, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_C", makeTS(2006, 8, 1), makeTS(2009, 12, 31), "any_reason"),
      FollowUp("Patient_F", makeTS(2006, 8, 1), makeTS(2009, 12, 31), "any_reason")
    ).toDS

    val expected = Seq(
      Patient("Patient_C", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_D", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_E", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_F", 0, makeTS(1945, 1, 1), None)
    ).toDS

    // When
    val instance = new PatientFiltersImplicits(patients)
    val result = instance.removeEarlyDiagnosedPatients(outcomes, followUpPeriods, "outcome_a")

    // Then
    assertDSs(result, expected)
  }

  "filterDelayedEntries" should "drop patients not exposed during the first N months of study" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val studyStart = makeTS(2006, 1, 1)
    val patients: Dataset[Patient] = Seq(
      Patient("Patient_A", 0, makeTS(1950, 1, 1), None),
      Patient("Patient_B", 0, makeTS(1940, 1, 1), Some(makeTS(2007, 1, 1))),
      Patient("Patient_C", 0, makeTS(1945, 1, 1), None)
    ).toDS

    val moleculeEvents: Dataset[Event[Molecule]] = Seq(
      Molecule("Patient_A", "molecule_a", 0.0, makeTS(2006, 5, 1)),
      Molecule("Patient_B", "molecule_a", 0.0, makeTS(2006, 6, 1)),
      Molecule("Patient_B", "molecule_a", 0.0, makeTS(2006, 7, 1)),
      Molecule("Patient_B", "molecule_b", 0.0, makeTS(2006, 8, 1)),
      Molecule("Patient_C", "molecule_b", 0.0, makeTS(2007, 2, 1)),
      Molecule("Patient_C", "molecule_a", 0.0, makeTS(2007, 9, 1)),
      Molecule("Patient_C", "molecule_b", 0.0, makeTS(2007, 10, 1))
    ).toDS

    val expected = Seq(
      Patient("Patient_A", 0, makeTS(1950, 1, 1), None),
      Patient("Patient_B", 0, makeTS(1940, 1, 1), Some(makeTS(2007, 1, 1)))
    ).toDS

    // When
    val instance = new PatientFiltersImplicits(patients)
    val result = instance.filterDelayedPatients(moleculeEvents, studyStart)

    // Then
    assertDSs(result, expected)
  }


  "filterNoStartGap" should "drop patients who were exposed during the first N months of study" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val studyStart = makeTS(2006, 1, 1)
    val patients: Dataset[Patient] = Seq(
      Patient("Patient_A", 0, makeTS(1950, 1, 1), None),
      Patient("Patient_B", 0, makeTS(1940, 1, 1), Some(makeTS(2007, 1, 1))),
      Patient("Patient_C", 0, makeTS(1945, 1, 1), None),
      Patient("Patient_D", 0, makeTS(1955, 1, 1), None)
    ).toDS

    val moleculeEvents: Dataset[Event[Molecule]] = Seq(
      Molecule("Patient_A", "molecule_a", 0.0, makeTS(2006, 1, 15)),
      Molecule("Patient_A", "molecule_a", 0.0, makeTS(2006, 2, 15)),
      Molecule("Patient_B", "molecule_a", 0.0, makeTS(2006, 3, 15)),
      Molecule("Patient_B", "molecule_b", 0.0, makeTS(2006, 5, 15)),
      Molecule("Patient_C", "molecule_b", 0.0, makeTS(2006, 2, 15)),
      Molecule("Patient_C", "molecule_b", 0.0, makeTS(2006, 3, 15)),
      Molecule("Patient_D", "molecule_b", 0.0, makeTS(2006, 4, 15)),
      Molecule("Patient_D", "molecule_a", 0.0, makeTS(2006, 5, 15)),
      Molecule("Patient_D", "molecule_b", 0.0, makeTS(2006, 6, 15))
    ).toDS

    val expected = Seq(
      Patient("Patient_B", 0, makeTS(1940, 1, 1), Some(makeTS(2007, 1, 1))),
      Patient("Patient_D", 0, makeTS(1955, 1, 1), None)
    ).toDS

    // When
    val instance = new PatientFiltersImplicits(patients)
    val result = instance.filterNoStartGap(moleculeEvents, studyStart)

    // Then
    assertDSs(result, expected)
  }
}