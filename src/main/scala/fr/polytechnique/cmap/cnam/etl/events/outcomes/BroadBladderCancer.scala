package fr.polytechnique.cmap.cnam.etl.events.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.diagnoses._

object BroadBladderCancer extends OutcomeTransformer {

  val outcomeName: String = "broad_bladder_cancer"

  val diagnosisCode: String = "C67"

  val directDiagnosisCategories = List(
    MainDiagnosis.category,
    LinkedDiagnosis.category,
    HADMainDiagnosis.category,
    SSRMainDiagnosis.category,
    SSREtiologicDiagnosis.category
  )

  val groupDiagnosisCategories = List(
    MainDiagnosis.category,
    LinkedDiagnosis.category
  )

  val groupDiagnosisValues = List("C77", "C78", "C79")

  def isDirectDiagnosis(ev: Event[Diagnosis]): Boolean = {
    directDiagnosisCategories.contains(ev.category) && ev.value == diagnosisCode
  }

  def isGroupDiagnosis(eventsGroup: Seq[Event[Diagnosis]]): Boolean = {
    eventsGroup.exists { ev =>
      ev.category == AssociatedDiagnosis.category && ev.value == diagnosisCode
    } &&
    eventsGroup.exists { ev =>
      groupDiagnosisCategories.contains(ev.category) && groupDiagnosisValues.contains(ev.value)
    }
  }

  implicit class BroadBladderCancerOutcome(ds: Dataset[Event[Diagnosis]]) {
    val sqlCtx = ds.sqlContext
    import sqlCtx.implicits._

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

  def transform(extracted: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {

    val directDiagnosisEvents: Dataset[Event[Diagnosis]] = extracted.filter(ev => isDirectDiagnosis(ev))

    val groupOutcomes: Dataset[Event[Outcome]] = extracted.filter(ev => !isDirectDiagnosis(ev)).groupOutcomes

    directDiagnosisEvents
        .directOutcomes
        .union(groupOutcomes)
  }
}
