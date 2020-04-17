// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, Molecule, ObservationPeriod}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._


/** It allows create a observationPeriod dataset using a dataset of type
 * [[fr.polytechnique.cmap.cnam.etl.events.AnyEvent]].
 *
 * @param config A config file that contains the need values to create the ObservationPeriod transformer.
 */
class ObservationPeriodTransformer(config: ObservationPeriodTransformerConfig) {

  import Columns._

  /** The main method in this transformation class, it allows the transformation into an ObservationPeriod dataset.
   *
   * @param events A dataset of [[fr.polytechnique.cmap.cnam.etl.events.AnyEvent]].
   * @return A dataset of Event[ObservationPeriod] type ([[fr.polytechnique.cmap.cnam.etl.events.ObservationPeriod]]).
   */
  def transform(events: Dataset[Event[AnyEvent]]): Dataset[Event[ObservationPeriod]] = {

    val studyStart: Timestamp = config.studyStart

    val studyEnd = config.studyEnd

    import events.sqlContext.implicits._

    /** It takes the events dataset and apply the following algorithm:
     * 1. Filter by category equal to ''molecule'' and start date before of study start date.
     * 2. Calculate the min of start date.
     * 3. Return an ObservationPeriod [[fr.polytechnique.cmap.cnam.etl.events.ObservationPeriod]].
     */
    events.filter(
      e => e.category == Molecule.category && (e.start
        .compareTo(studyStart) >= 0)
    )
      .groupBy(PatientID)
      .agg(min(Start).alias(Start))
      .map(
        e => ObservationPeriod(
          e.getAs[String](PatientID),
          e.getAs[Timestamp](Start),
          studyEnd
        )
      )
  }
}
