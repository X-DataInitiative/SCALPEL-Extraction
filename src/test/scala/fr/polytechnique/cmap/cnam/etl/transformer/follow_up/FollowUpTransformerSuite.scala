package fr.polytechnique.cmap.cnam.etl.transformer.follow_up

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.TimestampType

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.Diagnosis
import fr.polytechnique.cmap.cnam.etl.events.molecules.Molecule
import fr.polytechnique.cmap.cnam.etl.events.tracklosses.Trackloss
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions.makeTS

import fr.polytechnique.cmap.cnam.etl.transformer.observation.ObservationPeriod



class FollowUpTransformerSuite extends SharedContext {

  "withFollowUpStart" should "add a column with the start of the follow-up period" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import Columns._

    // Given
    val input = Seq(
      ("Patient_A", makeTS(2008, 1, 20), makeTS(2009, 12, 31)),
      ("Patient_A", makeTS(2008, 1, 20), makeTS(2009, 12, 31)),
      ("Patient_B", makeTS(2009, 1, 1), makeTS(2009, 12, 31)),
      ("Patient_B", makeTS(2009, 1, 1), makeTS(2009, 12, 31)),
      ("Patient_C", makeTS(2009, 10, 1), makeTS(2009, 12, 31)),
      ("Patient_C", makeTS(2009, 10, 1), makeTS(2009, 12, 31))
    ).toDF(PatientID, ObservationStart, ObservationEnd)

    val expected = Seq(
      ("Patient_A", Some(makeTS(2008, 7, 20))),
      ("Patient_A", Some(makeTS(2008, 7, 20))),
      ("Patient_B", Some(makeTS(2009, 7, 1))),
      ("Patient_B", Some(makeTS(2009, 7, 1))),
      ("Patient_C", None),
      ("Patient_C", None)
    ).toDF(PatientID, FollowUpStart)

    // When
    import FollowUpTransformer.FollowUpDataFrame
    val result = input.withFollowUpStart(6).select(PatientID, FollowUpStart)

    // Then
    assertDFs(result, expected)
  }


  "withTrackLoss" should "add the date of the right trackloss event" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import Columns._

    // Given
    val input = Seq(
      ("Patient_D", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF(PatientID, Category, Start, FollowUpStart)

    val expected = Seq(
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "disease", makeTS(2006, 8, 1))
    ).toDF(PatientID, Category, TracklossDate)

    // When
    import FollowUpTransformer.FollowUpDataFrame
    val result = input.withTrackloss.select(PatientID, Category, TracklossDate)

    // Then
    assertDFs(result, expected)
  }

  it should "get the first trackloss" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import Columns._

    // Given
    val input = Seq(
      ("Patient_D", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "trackloss", makeTS(2006, 12, 1), makeTS(2006, 6, 30)),
      ("Patient_D", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF(PatientID, Category, Start, FollowUpStart)

    val expected = Seq(
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "molecule", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "trackloss", makeTS(2006, 8, 1)),
      ("Patient_D", "disease", makeTS(2006, 8, 1))
    ).toDF(PatientID, Category, TracklossDate)

    // When
    import FollowUpTransformer.FollowUpDataFrame
    val result = input.withTrackloss.select(PatientID, Category, TracklossDate)

    // Then
    assertDFs(result, expected)
  }

  it should "avoid useless trackloss" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import Columns._

    // Given
    val input = Seq(
      ("Patient_C", "molecule", makeTS(2006, 2, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "molecule", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "trackloss", makeTS(2006, 1, 1), makeTS(2006, 6, 30)),
      ("Patient_C", "disease", makeTS(2007, 1, 1), makeTS(2006, 6, 30))
    ).toDF(PatientID, Category, Start, FollowUpStart)

    val expected = Seq(
      ("Patient_C", "molecule"),
      ("Patient_C", "molecule"),
      ("Patient_C", "trackloss"),
      ("Patient_C", "disease")
    ).toDF(PatientID, Category).withColumn(TracklossDate, lit(null).cast(TimestampType))

    // When
    import FollowUpTransformer.FollowUpDataFrame
    val result = input.withTrackloss.select(PatientID, Category, TracklossDate)

    // Then
    assertDFs(result, expected)
  }

  "withFollowUpEnd" should "add a column with the end of the follow-up period" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import Columns._

    // Given
    val input = Seq(
      // Cancer:
      ("Patient_A", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 12, 1), None),
      ("Patient_A", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 11, 1), None),
      ("Patient_A", Some(makeTS(2008, 1, 1)), "disease", "targetDisease", makeTS(2007, 12, 1), None),
      // Death:
      ("Patient_B", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 12, 1), None),
      ("Patient_B", Some(makeTS(2008, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 11, 1), None),
      // Track loss :
      ("Patient_C", Some(makeTS(2010, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 1, 1),
        Some(makeTS(2008, 2, 1))),
      ("Patient_C", Some(makeTS(2010, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2007, 2, 1),
        Some(makeTS(2008, 2, 1))),
      // End of Observation:
      ("Patient_D", Some(makeTS(2016, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2016, 1, 1), None),
      ("Patient_D", Some(makeTS(2016, 1, 1)), "molecule", "PIOGLITAZONE", makeTS(2016, 2, 1), None),
      ("Patient_D", Some(makeTS(2016, 1, 1)), "disease", "targetDisease", makeTS(2016, 3, 1), None)
    )
      .toDF(PatientID, "deathDate", Category, Value, Start, TracklossDate)
      .withColumn("observationEnd", lit(makeTS(2009, 12, 31, 23, 59, 59)))


    val expected = Seq(
      ("Patient_A", makeTS(2007, 12, 1)),
      ("Patient_A", makeTS(2007, 12, 1)),
      ("Patient_A", makeTS(2007, 12, 1)),
      ("Patient_B", makeTS(2008, 1, 1)),
      ("Patient_B", makeTS(2008, 1, 1)),
      ("Patient_C", makeTS(2008, 2, 1)),
      ("Patient_C", makeTS(2008, 2, 1)),
      ("Patient_D", makeTS(2009, 12, 31, 23, 59, 59)),
      ("Patient_D", makeTS(2009, 12, 31, 23, 59, 59)),
      ("Patient_D", makeTS(2009, 12, 31, 23, 59, 59))
    ).toDF(PatientID, FollowUpEnd)

    // When
    import FollowUpTransformer.FollowUpDataFrame
    val result = input.withFollowUpEnd.select(PatientID, FollowUpEnd)

    // Then
    assertDFs(result, expected)
  }

  "withEndReason" should "add a column for the reason of follow-up end" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    import Columns._

    // Given
    val input = Seq(
      (makeTS(2009, 12, 31), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2006, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2007, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2008, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2008, 12, 1),
        makeTS(2009, 12, 31)),
      (makeTS(2006, 12, 1), makeTS(2006, 12, 1), makeTS(2007, 12, 1), makeTS(2006, 12, 1),
        makeTS(2009, 12, 31))
    ).toDF(FollowUpEnd, DeathDate, FirstTargetDiseaseDate, TracklossDate, ObservationEnd)

    val expected = Seq(
      (makeTS(2006, 12, 1), "death"),
      (makeTS(2006, 12, 1), "death"),
      (makeTS(2007, 12, 1), "disease"),
      (makeTS(2008, 12, 1), "trackloss"),
      (makeTS(2009, 12, 31), "observationEnd")
    ).toDF(FollowUpEnd, EndReason)

    // When
    import FollowUpTransformer.FollowUpDataFrame
    val result = input.withEndReason.toDF.select(FollowUpEnd, EndReason)

    // Then
    assertDFs(result, expected)
  }

  "transform" should "return a Dataset[FollowUp] with the follow-up events of each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients = Seq(
      (Patient("Regis", 1, makeTS(1989, 10, 1), None), ObservationPeriod("Regis", makeTS(2006, 1, 1), makeTS(2009, 1, 1)))
    ).toDS

    val prescriptions = Seq(
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 1, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 2, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 3, 1))
    ).toDS

    val tracklosses = Seq.empty[Event[Trackloss]].toDS

    val outcomes = Seq.empty[Event[Diagnosis]].toDS

    val expected = Seq(
      FollowUp("Regis", makeTS(2006, 3, 1), makeTS(2009, 1, 1), "observationEnd")
    ).toDS

    val transformer = new FollowUpTransformer(2)

    // When
    val result = transformer.transform(patients, prescriptions, outcomes, tracklosses)

    // Then

    assertDSs(result, expected, true)
  }

}
