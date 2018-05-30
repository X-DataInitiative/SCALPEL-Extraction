package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer

class RosiglitazoneOutcomeTransformer(definition: OutcomeDefinition) extends OutcomesTransformer {

  val outcomeName: String = definition.outcomeName

  def transform(diagnoses: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {
    definition match {
      case OutcomeDefinition.Infarctus => Infarctus.transform(diagnoses)
      case OutcomeDefinition.HeartFailure => HeartFailure.transform(diagnoses)
    }
  }
}
