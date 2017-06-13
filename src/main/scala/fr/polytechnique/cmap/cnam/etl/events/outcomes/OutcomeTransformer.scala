package fr.polytechnique.cmap.cnam.etl.events.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

trait OutcomeTransformer {
  def transform(extracted: Dataset[Event[AnyEvent]]): Dataset[Event[Outcome]]
}
