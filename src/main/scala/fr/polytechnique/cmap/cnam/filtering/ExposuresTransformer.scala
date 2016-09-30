package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.utilities.functions._

trait ExposuresTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  // Constant definitions. Should be verified before compiling.
  // In the future, we may want to export them to an external file.
  val periodStart = makeTS(2006, 1, 1)

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent]
}
