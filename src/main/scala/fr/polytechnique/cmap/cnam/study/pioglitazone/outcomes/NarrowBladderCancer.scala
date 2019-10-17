// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneStudyCodes
import fr.polytechnique.cmap.cnam.util.functions._
import fr.polytechnique.cmap.cnam.util.{collections, datetime}

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=60093679
 */

object NarrowBladderCancer extends OutcomesTransformer with PioglitazoneStudyCodes {

  override val outcomeName: String = "narrow_bladder_cancer"

  private val DP = MainDiagnosis.category
  private val DR = LinkedDiagnosis.category
  private val DAS = AssociatedDiagnosis.category
  private val MCO_CIM_ACT = McoCIM10Act.category
  private val MCO_CAM_ACT = McoCCAMAct.category
  private val DCIR_CAM_ACT = DcirAct.category

  def transform(
    diagnoses: Dataset[Event[Diagnosis]],
    acts: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {

    import acts.sqlContext.implicits._

    val events = unionDatasets(diagnoses.as[Event[AnyEvent]], acts.as[Event[AnyEvent]])

    events
      .groupByKey(_.patientID)
      .flatMapGroups {
        case (_, eventsPerPatient: Iterator[Event[AnyEvent]]) => findOutcomes(eventsPerPatient)
      }
  }

  def findOutcomes(eventsOfPatient: Iterator[Event[AnyEvent]]): Seq[Event[Outcome]] = {

    // Grouping by hospitalization ID
    val groupedEvents: Map[String, Seq[Event[AnyEvent]]] = eventsOfPatient.toStream.groupBy(_.groupID)

    // We need all the dcir CAM codes
    val dcirCamEvents: Seq[Event[AnyEvent]] = groupedEvents.getOrElse(DCIR_CAM_ACT, Seq.empty)

    // Creates an outcome for each hospital stay with the right combination of events
    groupedEvents.collect {
      case (_, events) if checkHospitalStay(events, dcirCamEvents) =>
        Outcome(events.head.patientID, outcomeName, events.head.start)
    }.toStream
  }

  def checkHospitalStay(eventsInStay: Seq[Event[AnyEvent]], dcirCamEvents: Seq[Event[AnyEvent]]): Boolean = {
    eventsInStay.nonEmpty &&
      checkDates(eventsInStay) &&
      checkDiagnosesInStay(eventsInStay) && (
      checkMcoActsInStay(eventsInStay) || checkDcirActs(dcirCamEvents, eventsInStay.head.start)
      )
  }

  // Checks if all events in stay have the same dates
  def checkDates(eventsInStay: Seq[Event[AnyEvent]]): Boolean = {
    val areAllDatesEqual: Boolean = eventsInStay.forall(_.start == eventsInStay.head.start)
    if (!areAllDatesEqual) {
      val groupID = eventsInStay.head.groupID
      Logger.getLogger(getClass).warn(s"The dates for the GroupID: $groupID are not consistent")
    }
    areAllDatesEqual
  }

  def checkDiagnosesInStay(eventsInStay: Seq[Event[AnyEvent]]): Boolean = {

    import collections.implicits._

    eventsInStay.exists { e =>
      e.checkValueStart(DP, primaryDiagCode) ||
        e.checkValueStart(DR, primaryDiagCode) ||
        eventsInStay.existAll(
          e => e.checkValueStart(DAS, primaryDiagCode),
          e =>
            e.checkValueStart(DP, secondaryDiagCodes) ||
              e.checkValueStart(DR, secondaryDiagCodes)
        )
    }
  }

  def checkMcoActsInStay(eventsInStay: Seq[Event[AnyEvent]]): Boolean = {
    eventsInStay.exists { e =>
      e.checkValueStart(MCO_CIM_ACT, mcoCIM10ActCodes) ||
        e.checkValueStart(MCO_CAM_ACT, mcoCCAMActCodes)
    }
  }

  def checkDcirActs(dcirCamEvents: Seq[Event[AnyEvent]], stayDate: java.util.Date): Boolean = {
    import datetime.implicits._
    val filteredDcirEvents = dcirCamEvents.filter(_.checkValue(DCIR_CAM_ACT, dcirCCAMActCodes))
    filteredDcirEvents.exists {
      dcirEvent => dcirEvent.start.between(stayDate - 3.months, stayDate + 3.months)
    }
  }
}
