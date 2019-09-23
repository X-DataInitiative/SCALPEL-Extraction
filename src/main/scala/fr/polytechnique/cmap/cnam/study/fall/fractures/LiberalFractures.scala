package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer

object LiberalFractures extends OutcomesTransformer {

  override val outcomeName = "Liberal"

  def transform(events: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {
    import events.sqlContext.implicits._

    events
      .map(
        event => {
          val fractureSite = BodySite.getSiteFromCode(event.value, BodySites.sites, CodeType.CCAM)
          Outcome(event.patientID, fractureSite, outcomeName, event.weight, event.start)
        }
      )
  }

}
