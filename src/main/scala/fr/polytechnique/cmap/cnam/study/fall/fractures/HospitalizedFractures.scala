// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

case class HospitalStayID(patientID: String, groupID: String)

object HospitalizedFractures extends OutcomesTransformer with FractureCodes {

  override val outcomeName: String = "hospitalized_fall"

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
   * Filter out Diagnoses which do not have a MainDiagnosis during the same HospitalStay that is Fracture Diagnosis.
   *
   * @param diagnoses Fracture Diagnoses with DP, DA and DR diagnoses.
   * @return Diagnoses with a DP in the same hospital stay that is a fracture diagnosis.
   */
  def filterDiagnosesWithoutDP(diagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Diagnosis]] = {

    import diagnoses.sparkSession.implicits._

    diagnoses
      .groupByKey(_.groupID)
      .flatMapGroups { case (_, diagnoses) =>
        val diagnosisStream = diagnoses.toStream
        if (diagnosisStream.exists(_.category == McoMainDiagnosis.category)) {
          diagnosisStream
        } else {
          Seq.empty
        }
      }
  }

  /**
   * Get the ID of Hospital Stays that are mainly for fracture followup such as plaster removal.
   *
   * @param acts Contains all CCAM codes Events from different sources.
   * @return Hospital Stays id for fracture followup.
   */
  def getFractureFollowUpStays(acts: Dataset[Event[MedicalAct]]): Dataset[HospitalStayID] = {
    import acts.sparkSession.implicits._

    acts
      .filter(_.category == McoCCAMAct.category)
      .filter(isBadGHM _)
      .map(event => HospitalStayID(event.patientID, event.groupID))
      .distinct()
  }

  /**
   * Filter out Diagnosis who share a groupID in the followUpStaysForFractures.
   *
   * @param fracturesDiagnoses        Dataset of fracture diagnosis.
   * @param followUpStaysForFractures Dataset of hospital stays for followup of fractures.
   * @return
   */
  def filterDiagnosisForFracturesFollowUp(
    fracturesDiagnoses: Dataset[Event[Diagnosis]],
    followUpStaysForFractures: Dataset[HospitalStayID]
  ): Dataset[Event[Diagnosis]] = {
    import fracturesDiagnoses.sparkSession.implicits._
    fracturesDiagnoses
      .joinWith(
        broadcast(followUpStaysForFractures),
        fracturesDiagnoses(Event.Columns.PatientID) === followUpStaysForFractures("patientID")
          && fracturesDiagnoses(Event.Columns.GroupID) === followUpStaysForFractures("groupID"),
        "left_outer"
      )
      .filter(_._2 == null)
      .map(_._1)
  }

  def transform(
    diagnoses: Dataset[Event[Diagnosis]],
    acts: Dataset[Event[MedicalAct]],
    ghmSites: List[BodySite]
  ): Dataset[Event[Outcome]] = {

    import diagnoses.sqlContext.implicits._
    val ghmCodes = BodySite.extractCIM10CodesFromSites(ghmSites)
    val diagnosisWithDP = diagnoses.filter(diagnosis => isFractureDiagnosis(diagnosis, ghmCodes))
      .transform(filterDiagnosesWithoutDP)
    val fractureFollowUpHospitalStays = getFractureFollowUpStays(acts)


    filterDiagnosisForFracturesFollowUp(diagnosisWithDP, fractureFollowUpHospitalStays)
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
}
