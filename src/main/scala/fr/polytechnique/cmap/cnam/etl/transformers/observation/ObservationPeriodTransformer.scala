// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, Molecule, ObservationPeriod}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

class ObservationPeriodTransformer(config: ObservationPeriodTransformerConfig) {

  import Columns._

  def transform(events: Dataset[Event[AnyEvent]]): Dataset[Event[ObservationPeriod]] = {

    val studyStart: Timestamp = config.studyStart

    val studyEnd = config.studyEnd

    import events.sqlContext.implicits._

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
