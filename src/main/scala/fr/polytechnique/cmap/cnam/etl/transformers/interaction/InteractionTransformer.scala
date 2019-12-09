// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}

trait InteractionTransformer {
  def transform(exposures: Dataset[Event[Exposure]]): Dataset[Event[Interaction]]
}
