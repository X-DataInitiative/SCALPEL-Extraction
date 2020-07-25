// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import scala.util.Try
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerUtilities.{DeathReason, FollowUpEnd, ObservationEndReason, PatientDates, TrackLossDate, TrackLossReason, endReason, tracklossDateCorrected}
import fr.polytechnique.cmap.cnam.util.functions.makeTS


class FollowUpTransformerSuite extends SharedContext {

  case class FollowUpTestConfig(
    override val delayMonths: Int = 2,
    override val firstTargetDisease: Boolean = true,
    override val outcomeName: Option[String] = Some("cancer"))
    extends FollowUpTransformerConfig(delayMonths, firstTargetDisease, outcomeName)


  "cleanFollowUps" should "keep followups with only start < stop" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val followUpPeriods: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2009, 12, 31)),
      FollowUp("Patient_B", "any_reason", makeTS(2006, 7, 1), makeTS(2006, 7, 1)),
      FollowUp("Patient_C", "any_reason", makeTS(2016, 8, 1), makeTS(2009, 12, 31))

    ).toDS()

    val expected: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Patient_A", "any_reason", makeTS(2006, 6, 1), makeTS(2009, 12, 31))
    ).toDS()

    // When
    val result = followUpPeriods.filter(e => e.start.before(e.end.get))

    assertDSs(expected, result)
  }

  "correctedStart" should "Add month to observation start and correct the start of the follow-up period" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._


    // Given
    val input = Seq(
      ("Patient_A", makeTS(2008, 1, 20), Some(makeTS(2009, 12, 31))),
      ("Patient_A", makeTS(2008, 1, 20), Some(makeTS(2009, 12, 31))),
      ("Patient_B", makeTS(2009, 1, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_B", makeTS(2009, 1, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_C", makeTS(2009, 10, 1), Some(makeTS(2009, 12, 31))),
      ("Patient_C", makeTS(2009, 10, 1), Some(makeTS(2009, 12, 31)))
    ).toDS

    val expected = Seq(
      ("Patient_A", Some(makeTS(2008, 7, 20))),
      ("Patient_A", Some(makeTS(2008, 7, 20))),
      ("Patient_B", Some(makeTS(2009, 7, 1))),
      ("Patient_B", Some(makeTS(2009, 7, 1))),
      ("Patient_C", None),
      ("Patient_C", None)
    ).toDS

    // When
    import FollowUpTransformerUtilities.correctedStart

    val result = input.
      map(e => (e._1, correctedStart(e._2, e._3, 6)))
    // Then
    assertDSs(result, expected)
  }


  "tracklossDateCorrected" should "add the corrected date of the  trackloss event" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._


    // Given
    val input: Dataset[(PatientDates, Event[Trackloss])] = Seq(
      (PatientDates("Patient_A", Some(makeTS(2015, 2, 1)), Some(makeTS(2006, 2, 1)), Some(makeTS(2009, 6, 30))),
        Trackloss("Patient_A", makeTS(2006, 5, 1))),
      (PatientDates("Patient_B", Some(makeTS(2016, 2, 1)), Some(makeTS(2005, 1, 1)), Some(makeTS(2012, 6, 30))),
        Trackloss("Patient_B", makeTS(2008, 2, 1))),
      (PatientDates("Patient_C", Some(makeTS(2017, 2, 1)), Some(makeTS(2006, 8, 1)), Some(makeTS(2010, 6, 30))),
        Trackloss("Patient_C", makeTS(2006, 12, 1))),
      (PatientDates("Patient_D", Some(makeTS(2018, 2, 1)), Some(makeTS(2007, 1, 1)), Some(makeTS(2008, 6, 30))),
        Trackloss("Patient_D", makeTS(2007, 9, 1)))
    ).toDS

    val expected: Dataset[TrackLossDate] = Seq(
      TrackLossDate("Patient_A", Some(makeTS(2006, 5, 1))),
      TrackLossDate("Patient_B", Some(makeTS(2008, 2, 1))),
      TrackLossDate("Patient_C", Some(makeTS(2006, 12, 1))),
      TrackLossDate("Patient_D", Some(makeTS(2007, 9, 1)))
    ).toDS

    // When

    val result = input.map(
      e => TrackLossDate(
        e._2.patientID,
        tracklossDateCorrected(e._2.start, e._1.followUpStart.get)
      )
    )

    // Then
    assertDSs(result, expected)
  }


  it should "avoid useless trackloss" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[(PatientDates, Event[Trackloss])] = Seq(
      (PatientDates("Patient_A", Some(makeTS(2015, 2, 1)), Some(makeTS(2006, 2, 1)), Some(makeTS(2009, 6, 30))),
        Trackloss("Patient_A", makeTS(2006, 1, 1))),
      (PatientDates("Patient_B", Some(makeTS(2016, 2, 1)), Some(makeTS(2006, 1, 1)), Some(makeTS(2012, 6, 30))),
        Trackloss("Patient_B", makeTS(2005, 2, 1))),
      (PatientDates("Patient_C", Some(makeTS(2017, 2, 1)), Some(makeTS(2006, 8, 1)), Some(makeTS(2010, 6, 30))),
        Trackloss("Patient_C", makeTS(2005, 12, 1))),
      (PatientDates("Patient_D", Some(makeTS(2018, 2, 1)), Some(makeTS(2007, 10, 1)), Some(makeTS(2008, 6, 30))),
        Trackloss("Patient_D", makeTS(2007, 9, 1)))
    ).toDS

    val expected: Dataset[TrackLossDate] = Seq(
      TrackLossDate("Patient_A", None),
      TrackLossDate("Patient_B", None),
      TrackLossDate("Patient_C", None),
      TrackLossDate("Patient_D", None)
    ).toDS
    // When

    val result = input.map(
      e => TrackLossDate(
        e._2.patientID,
        tracklossDateCorrected(e._2.start, e._1.followUpStart.get)
      )
    )

    // Then
    assertDSs(result, expected)
  }


  "withEndReason" should "add a column for the reason of follow-up end" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: Dataset[(PatientDates, TrackLossDate)] = Seq(
      (PatientDates("Patient_A", Some(makeTS(2015, 2, 1)), Some(makeTS(2006, 2, 1)), Some(makeTS(2014, 5, 1))),
        TrackLossDate("Patient_A", Some(makeTS(2014, 5, 1))))
      ,
      (PatientDates("Patient_B", Some(makeTS(2016, 2, 1)), Some(makeTS(2006, 1, 1)), Some(makeTS(2012, 6, 30))),
        TrackLossDate("Patient_B", Some(makeTS(2010, 2, 1)))),
      (PatientDates("Patient_C", Some(makeTS(2017, 2, 1)), Some(makeTS(2006, 8, 1)), Some(makeTS(2017, 2, 1))),
        TrackLossDate("Patient_C", None)),

      (PatientDates("Patient_D", Some(makeTS(2018, 2, 1)), Some(makeTS(2007, 10, 1)), Some(makeTS(2013, 6, 30))),
        TrackLossDate("Patient_D", Some(makeTS(2017, 9, 1))))
    ).toDS

    // When
    val result: Dataset[FollowUpEnd] = input
      .map { e =>
        endReason(
          DeathReason(date = e._1.deathDate),
          TrackLossReason(date = Try(e._2.trackloss).getOrElse(None)),
          ObservationEndReason(date = e._1.observationEnd)
        )
      }

    val expected: Dataset[FollowUpEnd] = Seq(
      FollowUpEnd("Trackloss", Some(makeTS(2014, 5, 1))),
      FollowUpEnd("Trackloss", Some(makeTS(2010, 2, 1))),
      FollowUpEnd("Death", Some(makeTS(2017, 2, 1))),
      FollowUpEnd("ObservationEnd", Some(makeTS(2013, 6, 30)))
    ).toDS


    // Then
    assertDSs(result, expected)

  }

  "transform" should "return a Dataset[FollowUp] with the follow-up events of each patient with Cox model" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients = Seq(
      (Patient("Regis", 1, makeTS(1989, 10, 1), None), ObservationPeriod(
        "Regis",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      )),
      (Patient("pika", 1, makeTS(1980, 10, 1), Some(makeTS(2008, 10, 1))), ObservationPeriod(
        "pika",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      )),
      (Patient("patient03", 1, makeTS(1980, 10, 1), Some(makeTS(2010, 10, 1))), ObservationPeriod(
        "patient03",
        makeTS(2006, 1, 1),
        makeTS(2009, 2, 1)
      ))
    ).toDS


    val prescriptions = Seq(
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 1, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 2, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 3, 1)),
      Molecule("pika", "doliprane", 200.00, makeTS(2007, 5, 1)),
      Molecule("patient03", "doliprane", 200.00, makeTS(2007, 5, 1))
    ).toDS

    val tracklosses: Dataset[Event[Trackloss]] = Seq(
      Trackloss("Regis", makeTS(2006, 3, 30)),
      Trackloss("pika", makeTS(2006, 3, 30))
    ).toDS

    val outcomes = Seq(
      Outcome("Regis", "bladder_cancer", makeTS(2007, 9, 1)),
      Outcome("Regis", "bladder_cancer", makeTS(2008, 4, 1)),
      Outcome("pika", "cancer", makeTS(2010, 1, 1)),
      Outcome("patient03", "fall", makeTS(2010, 1, 1))
    ).toDS

    val expected: Dataset[Event[FollowUp]] = Seq(
      FollowUp("Regis", "Trackloss", makeTS(2006, 3, 1), makeTS(2006, 3, 30)),
      FollowUp("pika", "Trackloss", makeTS(2006, 3, 1), makeTS(2006, 3, 30)),
      FollowUp("patient03", "ObservationEnd", makeTS(2006, 3, 1), makeTS(2009, 2, 1))
    ).toDS

    val transformer = new FollowUpTransformer(FollowUpTestConfig())

    // When
    val result: Dataset[Event[FollowUp]] = transformer.transform(patients, prescriptions, outcomes, tracklosses)


    // Then
    assertDSs(result, expected)
  }

  "transform" should "return a Dataset[FollowUp] with the follow-up events of each patient with Tick model" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients = Seq(
      (Patient("Regis", 1, makeTS(1989, 10, 1), None), ObservationPeriod(
        "Regis",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      )),
      (Patient("pika", 1, makeTS(1980, 10, 1), Some(makeTS(2008, 10, 1))), ObservationPeriod(
        "pika",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      )),
      (Patient("patient03", 1, makeTS(1980, 10, 1), Some(makeTS(2010, 10, 1))), ObservationPeriod(
        "pika",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      ))
    ).toDS

    val prescriptions = Seq(
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 1, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 2, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 3, 1)),
      Molecule("pika", "doliprane", 200.00, makeTS(2007, 5, 1)),
      Molecule("patient03", "doliprane", 200.00, makeTS(2007, 5, 1))
    ).toDS

    val tracklosses = Seq.empty[Event[Trackloss]].toDS

    val outcomes = Seq(
      Outcome("Regis", "bladder_cancer", makeTS(2007, 9, 1)),
      Outcome("Regis", "bladder_cancer", makeTS(2008, 4, 1)),
      Outcome("pika", "cancer", makeTS(2010, 1, 1)),
      Outcome("patient03", "fall", makeTS(2010, 1, 1))
    ).toDS

    val expected = Seq(
      FollowUp("Regis", "ObservationEnd", makeTS(2006, 3, 1), makeTS(2009, 1, 1)),
      FollowUp("pika", "Death", makeTS(2006, 3, 1), makeTS(2008, 10, 1)),
      FollowUp("patient03", "ObservationEnd", makeTS(2006, 3, 1), makeTS(2009, 1, 1))
    ).toDS

    val transformer = new FollowUpTransformer(FollowUpTestConfig(firstTargetDisease = false))

    // When
    val result = transformer.transform(patients, prescriptions, outcomes, tracklosses)

    // Then
    assertDSs(result, expected)
  }

  "transform" should "return a Dataset[FollowUp] with the follow-up events of each patient with LCSCCS model" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val patients = Seq(
      (Patient("Regis", 1, makeTS(1989, 10, 1), None), ObservationPeriod(
        "Regis",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      )),
      (Patient("pika", 1, makeTS(1980, 10, 1), Some(makeTS(2008, 10, 1))), ObservationPeriod(
        "pika",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      )),
      (Patient("patient03", 1, makeTS(1980, 10, 1), Some(makeTS(2010, 10, 1))), ObservationPeriod(
        "pika",
        makeTS(2006, 1, 1),
        makeTS(2009, 1, 1)
      ))
    ).toDS

    val prescriptions = Seq(
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 1, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 2, 1)),
      Molecule("Regis", "doliprane", 200.00, makeTS(2007, 3, 1)),
      Molecule("pika", "doliprane", 200.00, makeTS(2007, 5, 1)),
      Molecule("patient03", "doliprane", 200.00, makeTS(2007, 5, 1))
    ).toDS

    val tracklosses = Seq.empty[Event[Trackloss]].toDS

    val outcomes = Seq.empty[Event[Outcome]].toDS
    val expected = Seq(
      FollowUp("Regis", "ObservationEnd", makeTS(2006, 3, 1), makeTS(2009, 1, 1)),
      FollowUp("pika", "Death", makeTS(2006, 3, 1), makeTS(2008, 10, 1)),
      FollowUp("patient03", "ObservationEnd", makeTS(2006, 3, 1), makeTS(2009, 1, 1))
    ).toDS

    val transformer = new FollowUpTransformer(FollowUpTestConfig(firstTargetDisease = false))

    // When
    val result = transformer.transform(patients, prescriptions, outcomes, tracklosses)

    // Then
    assertDSs(result, expected)
  }
}
