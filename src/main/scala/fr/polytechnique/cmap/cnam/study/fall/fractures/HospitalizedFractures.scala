// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

case class HospitalStayID(patientID: String, id: String)

object HospitalizedFractures extends OutcomesTransformer with FractureCodes {

  override val outcomeName: String = "hospitalized_fall"

  def transform(
    diagnoses: Dataset[Event[Diagnosis]],
    acts: Dataset[Event[MedicalAct]], ghmSites: List[BodySite]): Dataset[Event[Outcome]] = {

    import diagnoses.sqlContext.implicits._
    val ghmCodes = BodySite.extractCIM10CodesFromSites(ghmSites)
    val correctCIM10Event = diagnoses
      .filter(diagnosis => isFractureDiagnosis(diagnosis, ghmCodes))

    val incorrectGHMStays = acts
      .filter(isBadGHM _)
      .map(event => HospitalStayID(event.patientID, event.groupID))
      .distinct()

    filterHospitalStay(correctCIM10Event, incorrectGHMStays)
      .map(
        event => Outcome(
          event.patientID,
          BodySite.getSiteFromCode(event.value, ghmSites, CodeType.CIM10),
          outcomeName,
          event.weight,
          event.start
        )
      )

  }

  def isFractureDiagnosis(event: Event[Diagnosis], ghmSites: List[String]): Boolean = {
    isInCodeList(event, ghmSites.toSet)
  }

  def isBadGHM(event: Event[MedicalAct]): Boolean = {
    isInCodeList(event, CCAMExceptions)
  }

  def isInCodeList[T <: AnyEvent](event: Event[T], codes: Set[String]): Boolean = {
    codes.exists(event.value.startsWith)
  }

  /**
   * filters diagnosis that do not have a DP in the same hospital stay
   * and the diagnosis that relates to an incorrectGHMStay
   */
  def filterHospitalStay(
    events: Dataset[Event[Diagnosis]],
    incorrectGHMStays: Dataset[HospitalStayID])
  : Dataset[Event[Diagnosis]] = {

    val spark: SparkSession = events.sparkSession
    import spark.implicits._
    val fracturesDiagnoses = events
      .groupByKey(_.groupID)
      .flatMapGroups { case (_, diagnoses) =>
        val diagnosisStream = diagnoses.toStream
        if (diagnosisStream.exists(_.category == MainDiagnosis.category)) {
          diagnosisStream
        } else {
          Seq.empty
        }
      }.toDF()


    val patientsToFilter = incorrectGHMStays.select("patientID")
    fracturesDiagnoses
      .join(broadcast(patientsToFilter), Seq("patientID"), "left_anti")
      .as[Event[Diagnosis]]
  }

  def isMainOrDASDiagnosis(event: Event[Diagnosis]): Boolean = {
    event.category == MainDiagnosis.category || event.category == AssociatedDiagnosis.category
  }

}
