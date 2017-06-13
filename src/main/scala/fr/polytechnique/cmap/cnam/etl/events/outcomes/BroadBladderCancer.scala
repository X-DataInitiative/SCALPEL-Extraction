package fr.polytechnique.cmap.cnam.etl.events.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

object BroadBladderCancer extends OutcomeTransformer {

  val outcomeName: String = "broad_bladder_cancer"

  val diagnosisCode: String = "C67"

  val directDiagnosisCategories = List(
    MainDiagnosis.category,
    LinkedDiagnosis.category,
    HADMainDiagnosis.category,
    SSRMainDiagnosis.category,
    SSREtiologiqueDiagnosis.category
  )

  val groupDiagnosisCategories = List(
    MainDiagnosis.category,
    LinkedDiagnosis.category
  )

  val groupDiagnosisValues = List("C77", "C78", "C79")

  def isDirectDiagnosis(ev: Event[AnyEvent]): Boolean = {
    directDiagnosisCategories.contains(ev.category) && ev.value == diagnosisCode
  }

  def isGroupDiagnosis(eventsGroup: Iterator[Event[AnyEvent]]): Boolean = {
    eventsGroup.exists { ev =>
      ev.category == AssociatedDiagnosis.category && ev.value == diagnosisCode
    } &&
    eventsGroup.exists { ev =>
      groupDiagnosisCategories.contains(ev.category) && groupDiagnosisValues.contains(ev.value)
    }
  }

  implicit class BroadBladderCancerOutcome(ds: Dataset[Event[AnyEvent]]) {
    val sqlCtx = ds.sqlContext
    import sqlCtx.implicits._

    def directOutcomes: Dataset[Event[Outcome]] = {
      ds
        .map(event => Outcome(event.patientID, outcomeName, event.start))
    }

    def groupOutcomes: Dataset[Event[Outcome]] = {
      ds
        .groupByKey(ev => (ev.patientID, ev.groupID, ev.start))
        .flatMapGroups {
          case ((patID, grpID, start), eventsInGroup: Iterator[Event[AnyEvent]]) =>
            if (isGroupDiagnosis(eventsInGroup)) {
              List(Outcome(patID, outcomeName, start))
            } else {
              List[Event[Outcome]]()
            }
        }
    }
  }

  override def transform(extracted: Dataset[Event[AnyEvent]]): Dataset[Event[Outcome]] = {

    val directDiagnosisEvents: Dataset[Event[AnyEvent]] = extracted.filter(ev => isDirectDiagnosis(ev))

    val groupOutcomes: Dataset[Event[Outcome]] = extracted.except(directDiagnosisEvents).groupOutcomes

    directDiagnosisEvents
        .directOutcomes
        .union(groupOutcomes)
  }
}
