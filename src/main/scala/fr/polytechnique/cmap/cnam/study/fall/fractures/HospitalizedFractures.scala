// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes
import fr.polytechnique.cmap.cnam.study.fall.extractors.Death
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

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
  def filterDiagnosisForFracturesFollowUp(followUpStaysForFractures: Dataset[HospitalStayID])
    (fracturesDiagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Diagnosis]] = {
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

  def getFourthLevelSeverity(stays: Dataset[Event[HospitalStay]])
    (diagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Diagnosis]] = {
    import stays.sparkSession.implicits._
    diagnoses.joinWith(
      stays.filter(_.value == Death.value),
      diagnoses(Event.Columns.PatientID) === stays(Event.Columns.PatientID)
        && diagnoses(Event.Columns.GroupID) === stays(Event.Columns.GroupID),
      "inner"
    )
      .map(_._1)
  }

  def getThirdLevelSeverity(surgeries: Dataset[Event[MedicalAct]])
    (diagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Diagnosis]] = {
    import surgeries.sparkSession.implicits._
    diagnoses.joinWith(
      surgeries,
      diagnoses(Event.Columns.PatientID) === surgeries(Event.Columns.PatientID)
        && diagnoses(Event.Columns.GroupID) === surgeries(Event.Columns.GroupID),
      "inner"
    )
      .map(_._1)
  }

  def assignSeverityToDiagnosis(stays: Dataset[Event[HospitalStay]], surgeries: Dataset[Event[MedicalAct]])
    (diagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Diagnosis]] = {

    val fourthLevelSeverity = diagnoses.transform(getFourthLevelSeverity(stays)).cache()

    val notFourthLevel = diagnoses.except(fourthLevelSeverity).cache()
    val thirdLevelSeverity = notFourthLevel
      .transform(getThirdLevelSeverity(surgeries)).cache()

    val secondLevelSeverity = notFourthLevel.except(thirdLevelSeverity)
    import surgeries.sparkSession.implicits._
    unionDatasets(
      fourthLevelSeverity.map(_.copy(weight = 4D)),
      thirdLevelSeverity.map(_.copy(weight = 3D)),
      secondLevelSeverity.map(_.copy(weight = 2D))
    )
  }

  def transform(
    diagnoses: Dataset[Event[Diagnosis]],
    acts: Dataset[Event[MedicalAct]],
    stays: Dataset[Event[HospitalStay]],
    surgeries: Dataset[Event[MedicalAct]],
    ghmSites: List[BodySite]
  ): Dataset[Event[Outcome]] = {

    import diagnoses.sqlContext.implicits._
    val ghmCodes = BodySite.extractCIM10CodesFromSites(ghmSites)
    val diagnosisWithDP = diagnoses.filter(diagnosis => isFractureDiagnosis(diagnosis, ghmCodes))
      .transform(filterDiagnosesWithoutDP)
    val fractureFollowUpHospitalStays = getFractureFollowUpStays(acts)


    diagnosisWithDP
      .transform(filterDiagnosisForFracturesFollowUp(fractureFollowUpHospitalStays))
      .transform(assignSeverityToDiagnosis(stays, surgeries))
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
