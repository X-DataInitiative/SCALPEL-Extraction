package fr.polytechnique.cmap.cnam.study.pioglitazone

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.acts._
import fr.polytechnique.cmap.cnam.etl.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.events.outcomes.{Outcome, OutcomeTransformer}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}
import fr.polytechnique.cmap.cnam.util.functions._
import fr.polytechnique.cmap.cnam.util.{collections, datetime}

object NarrowBladderCancer extends OutcomeTransformer {

  val name: String = "narrow_bladder_cancer"
  val primaryDiagCode: String = "C67"
  val secondaryDiagCodes: List[String] = List("C77", "C78", "C79")
  val mcoActCIMCodes: List[String] = List(
    "Z511", // Radiotherapy
    "Z510" // Chemotherapy
  )
  val mcoActCAMCodes: List[String] = List(
    "JDFA001", "JDFA003", "JDFA004", "JDFA005", "JDFA006", "JDFA008", "JDFA009", "JDFA011",
      "JDFA014", "JDFA015", "JDFA016", "JDFA017", "JDFA018", "JDFA019", "JDFA020", "JDFA021",
      "JDFA022", "JDFA023", "JDFA024", "JDFA025", "JDFC023", // List 1
    "JDLD002" // List 2
  )
  val dcirCamCodes: List[String] = List(
    "YYYY045", "YYYY099", "YYYY101", "YYYY046", "YYYY136", "YYYY312", "YYYY047", "YYYY152",
    "YYYY323", "YYYY048", "YYYY211", "YYYY334", "YYYY049", "YYYY197", "YYYY345", "YYYY050",
    "YYYY244", "YYYY356", "YYYY301", "YYYY302", "YYYY313", "YYYY324", "YYYY343", "YYYY335",
    "YYYY346", "YYYY357", "YYYY367", "YYYY368", "YYYY379", "YYYY383", "YYYY390", "YYYY392",
    "YYYY457", "YYYY468", "YYYY471", "YYYY479", "YYYY497", "YYYY303", "YYYY310", "YYYY314",
    "YYYY325", "YYYY336", "YYYY307", "YYYY347", "YYYY358", "YYYY369", "YYYY380", "YYYY387",
    "YYYY391", "YYYY458", "YYYY460", "YYYY469", "YYYY480", "YYYY491", "YYYY299", "YYYY304",
    "YYYY315", "YYYY326", "YYYY331", "YYYY337", "YYYY348", "YYYY359", "YYYY370", "YYYY377",
    "YYYY381", "YYYY398", "YYYY450", "YYYY459", "YYYY470", "YYYY481", "YYYY493", "YYYY492",
    "YYYY305", "YYYY316", "YYYY320", "YYYY327", "YYYY338", "YYYY349", "YYYY360", "YYYY365",
    "YYYY371", "YYYY382", "YYYY451", "YYYY393", "YYYY500", "YYYY511", "YYYY520", "YYYY522",
    "YYYY533", "YYYY544", "YYYY051", "YYYY122", "YYYY555", "YYYY052", "YYYY053", "YYYY054",
    "YYYY055", "YYYY056", "YYYY141", "YYYY175", "YYYY223", "YYYY256", "YYYY267", "YYYY566",
    "YYYY577", "YYYY588", "YYYY599", "YYYY306", "YYYY016", "YYYY021", "YYYY023"
  )

  private val DP = MainDiagnosis.category
  private val DR = LinkedDiagnosis.category
  private val DAS = AssociatedDiagnosis.category
  private val MCO_CIM_ACT = McoCimAct.category
  private val MCO_CAM_ACT = McoCamAct.category
  private val DCIR_CAM_ACT = DcirCamAct.category

  // Checks if all events in stay have the same dates
  def checkDates(eventsInStay: Seq[Event[AnyEvent]]): Boolean = {
    val res = !eventsInStay.exists(_.start != eventsInStay.head.start)
    if(!res) {
      val groupID = eventsInStay.head.groupID
      Logger.getLogger(getClass).warn(s"The dates for the GroupID: $groupID are not consistent")
    }
    res
  }

  def checkDiagnosesInStay(eventsInStay: Seq[Event[AnyEvent]]): Boolean = {

    import collections.implicits._

    eventsInStay.exists { e =>
      e.checkValue(DP, "C67") ||
      e.checkValue(DR, "C67") ||
      eventsInStay.existAll(
        e => e.checkValue(DAS, "C67"),
        e =>
          e.checkValue(DP, Seq("C77", "C78", "C79")) ||
          e.checkValue(DR, Seq("C77", "C78", "C79"))
      )
    }
  }

  def checkMcoActsInStay(eventsInStay: Seq[Event[AnyEvent]]): Boolean = {
    eventsInStay.exists { e =>
      e.checkValue(MCO_CIM_ACT, Seq("Z510", "Z511")) ||
      e.checkValue(MCO_CAM_ACT, mcoActCAMCodes)
    }
  }

  def checkDcirActs(dcirCamEvents: Seq[Event[AnyEvent]], stayDate: java.util.Date): Boolean = {
    import datetime.implicits._
    val filteredDcirEvents = dcirCamEvents.filter(_.checkValue(DCIR_CAM_ACT, dcirCamCodes))
    filteredDcirEvents.exists {
      dcirEvent => dcirEvent.start.between(stayDate - 3.months, stayDate + 3.months)
    }
  }

  def checkHospitalStay(eventsInStay: Seq[Event[AnyEvent]], dcirCamEvents: Seq[Event[AnyEvent]]): Boolean = {
    checkDates(eventsInStay) &&
    checkDiagnosesInStay(eventsInStay) && (
      checkMcoActsInStay(eventsInStay) || checkDcirActs(dcirCamEvents, eventsInStay.head.start)
    )
  }

  def findOutcomes(eventsOfPatient: Iterator[Event[AnyEvent]]): Seq[Event[Outcome]] = {

    // Grouping by hospitalization ID
    val groupedEvents: Map[String, Seq[Event[AnyEvent]]] = eventsOfPatient.toStream.groupBy(_.groupID)

    // We need all the dcir CAM codes
    val dcirCamEvents: Seq[Event[AnyEvent]] = groupedEvents.getOrElse(DCIR_CAM_ACT, Seq.empty)

    // Creates an outcome for each hospital stay with the right combination of events
    groupedEvents.collect {
      case (_, events) if checkHospitalStay(events, dcirCamEvents) =>
        Outcome(events.head.patientID, name, events.head.start)
    }.toStream
  }

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
}
