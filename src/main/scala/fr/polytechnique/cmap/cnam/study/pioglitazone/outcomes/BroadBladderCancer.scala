package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneStudyCodes

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=60093165
 */

object BroadBladderCancer extends OutcomesTransformer with PioglitazoneStudyCodes {

  override val outcomeName: String = "broad_bladder_cancer"

  val directDiagnosisCategories = List(
    McoMainDiagnosis.category,
    McoLinkedDiagnosis.category,
    HadMainDiagnosis.category,
    SsrMainDiagnosis.category,
    SsrAssociatedDiagnosis.category
  )

  val groupDiagnosisCategories = List(
    McoMainDiagnosis.category,
    McoLinkedDiagnosis.category
  )

  def isGroupDiagnosis(eventsGroup: Seq[Event[Diagnosis]]): Boolean = {
    eventsGroup.exists { ev =>
      ev.category == McoAssociatedDiagnosis.category && ev.value == primaryDiagCode
    } &&
      eventsGroup.exists { ev =>
        groupDiagnosisCategories.contains(ev.category) && secondaryDiagCodes.contains(ev.value)
      }
  }

  def transform(extracted: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {

    val directDiagnosisEvents: Dataset[Event[Diagnosis]] = extracted.filter(ev => isDirectDiagnosis(ev))

    val groupOutcomes: Dataset[Event[Outcome]] = extracted.filter(ev => !isDirectDiagnosis(ev)).groupOutcomes

    directDiagnosisEvents
      .directOutcomes
      .union(groupOutcomes)
  }

  implicit class BroadBladderCancerOutcome(ds: Dataset[Event[Diagnosis]]) {

    import ds.sqlContext.implicits._

    def directOutcomes: Dataset[Event[Outcome]] = {
      ds.map(event => Outcome(event.patientID, outcomeName, event.start))
    }

    def groupOutcomes: Dataset[Event[Outcome]] = {
      ds
        .groupByKey(ev => (ev.patientID, ev.groupID, ev.start))
        .flatMapGroups {
          case ((patID, grpID, start), eventsInGroup: Iterator[Event[Diagnosis]]) =>
            if (isGroupDiagnosis(eventsInGroup.toStream)) {
              List(Outcome(patID, outcomeName, start))
            } else {
              List[Event[Outcome]]()
            }
        }
    }
  }

  def isDirectDiagnosis(ev: Event[Diagnosis]): Boolean = {
    directDiagnosisCategories.contains(ev.category) && ev.value == primaryDiagCode
  }
}
