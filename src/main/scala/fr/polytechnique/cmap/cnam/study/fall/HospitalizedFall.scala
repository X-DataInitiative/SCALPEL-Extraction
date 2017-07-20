package fr.polytechnique.cmap.cnam.study.fall

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer
import org.apache.spark.sql

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

case class HospitalStay(patientID: String, ID: String)

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

  // T <: AnyEvent : TypeTag
  // means that we need a subtype of AnyEvent
  // And that we need to pass TypeTag as an implicit parameter to convert
  // back to the correct type.
  // cf. https://stackoverflow.com/questions/12218641/scala-what-is-a-typetag-and-how-do-i-use-it
  def filterHospitalStay[T <: AnyEvent : TypeTag](
      events: Dataset[Event[T]],
      stays: Dataset[HospitalStay])
    : Dataset[Event[T]] = {
    import events.sqlContext.implicits._

    events
      .joinWith(stays, events("patientID") === stays("patientID"), "left_outer")
      .filter(_._2 == null)
      .map(_._1)
  }

  def transform(
      diagnoses: Dataset[Event[Diagnosis]],
      classifications: Dataset[Event[Classification]]
    ): Dataset[Event[Outcome]] = {
    import diagnoses.sqlContext.implicits._

    val correctCIM10event = diagnoses
      .filter(isMainDiagnosis _)
      .filter(isFractureDiagnosis _)

    val incorrectGHMstays = classifications
      .filter(isBadGHM _)
      .map(event => HospitalStay(event.patientID, event.groupID))
      .distinct()

    filterHospitalStay(correctCIM10event, incorrectGHMstays)
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }

}
