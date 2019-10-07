// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, MedicalAct, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer

class PioglitazoneOutcomeTransformer(definition: CancerDefinition) extends OutcomesTransformer {

  val outcomeName: String = definition match {
    case CancerDefinition.Broad => BroadBladderCancer.outcomeName
    case CancerDefinition.Naive => NaiveBladderCancer.outcomeName
    case CancerDefinition.Narrow => NarrowBladderCancer.outcomeName
  }

  def transform(diagnoses: Dataset[Event[Diagnosis]], acts: Dataset[Event[MedicalAct]])
  : Dataset[Event[Outcome]] = {

    definition match {
      case CancerDefinition.Broad => BroadBladderCancer.transform(diagnoses)
      case CancerDefinition.Naive => NaiveBladderCancer.transform(diagnoses)
      case CancerDefinition.Narrow => NarrowBladderCancer.transform(diagnoses, acts)
    }
  }
}
