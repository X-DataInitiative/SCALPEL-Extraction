package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes

import scala.collection.immutable.Stream.Empty

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

case class HospitalStay(patientID: String, id: String)

object HospitalizedFractures extends OutcomeTransformer with FractureCodes {

  override val outcomeName: String = "hospitalized_fall"

  def isInCodeList[T <: AnyEvent](event: Event[T], codes: Set[String]): Boolean = {
    codes.exists(event.value.startsWith)
  }

  def isFractureDiagnosis(event: Event[Diagnosis], ghmSites: List[String]): Boolean = {
     isInCodeList(event, ghmSites.toSet)
  }

  def isMainOrDASDiagnosis(event: Event[Diagnosis]): Boolean = {
    event.category == MainDiagnosis.category || event.category == AssociatedDiagnosis.category
  }

  def isBadGHM(event: Event[MedicalAct]): Boolean = {
    isInCodeList(event, CCAMExceptions)
  }



  def filterHospitalStay(
      events: Dataset[Event[Diagnosis]],
      stays: Dataset[HospitalStay])
    : Dataset[Event[Diagnosis]] = {

    val spark: SparkSession = events.sparkSession
    import spark.implicits._
    val fracturesDiagnoses = events
      .rdd.groupBy(_.groupID)
      .flatMap{
      //.flatMapGroups{
        case(_, diagnoses) if (diagnoses.exists(_.category == MainDiagnosis.category)) => diagnoses
        case _ => Seq.empty}.toDF()

    val patientsToFilter = stays.select("patientID")
    fracturesDiagnoses
      .join(broadcast(patientsToFilter), Seq("patientID"), "left_anti")
      .as[Event[Diagnosis]]
  }

  def transform(
      diagnoses: Dataset[Event[Diagnosis]],
      acts: Dataset[Event[MedicalAct]], ghmSites: List[Site]): Dataset[Event[Outcome]] = {

    import diagnoses.sqlContext.implicits._
    val ghmCodes = Site.extractCodeSites(ghmSites)
    val correctCIM10Event = diagnoses
      .filter(isMainOrDASDiagnosis _)
      .filter(diagnosis => isFractureDiagnosis(diagnosis, ghmCodes))

    val incorrectGHMStays = acts
      .filter(isBadGHM _)
      .map(event => HospitalStay(event.patientID, event.groupID))
      .distinct()

    filterHospitalStay(correctCIM10Event, incorrectGHMStays)
      .map(event => Outcome(event.patientID, Site.getSiteFromCode(event.value, ghmSites), outcomeName, event.start))

  }

}
