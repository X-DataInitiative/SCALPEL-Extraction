package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

case class HospitalStay(patientID: String, id: String)

object HospitalizedFall extends OutcomeTransformer with FallStudyCodes {

  override val outcomeName: String = "hospitalized_fall"

  def isInCodeList[T <: AnyEvent](event: Event[T], codes: List[String]): Boolean = {
    codes.map(event.value.startsWith).exists(identity)
  }

  def isFractureDiagnosis(event: Event[Diagnosis]): Boolean = {
    isInCodeList(event, GenericCIM10Codes)
  }

  def isMainDiagnosis(event: Event[Diagnosis]): Boolean = {
    event.category == MainDiagnosis.category
  }

  def isBadGHM(event: Event[Classification]): Boolean = {
    isInCodeList(event, GenericGHMCodes)
  }

  def filterHospitalStay(
      events: Dataset[Event[Diagnosis]],
      stays: Dataset[HospitalStay])
    : Dataset[Event[Diagnosis]] = {

    import events.sqlContext.implicits._
    val patientsToFilter = stays.select("patientID")
    events
      .join(broadcast(patientsToFilter), Seq("patientID"), "left_anti")
      .as[Event[Diagnosis]]
  }

  def transform(
      diagnoses: Dataset[Event[Diagnosis]],
      classifications: Dataset[Event[Classification]]): Dataset[Event[Outcome]] = {

    import diagnoses.sqlContext.implicits._

    val correctCIM10Event = diagnoses
      .filter(isMainDiagnosis _)
      .filter(isFractureDiagnosis _)

    val incorrectGHMStays = classifications
      .filter(isBadGHM _)
      .map(event => HospitalStay(event.patientID, event.groupID))
      .distinct()

    filterHospitalStay(correctCIM10Event, incorrectGHMStays)
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }

}
