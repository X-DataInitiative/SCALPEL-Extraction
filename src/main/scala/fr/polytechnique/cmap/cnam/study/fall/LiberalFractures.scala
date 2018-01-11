package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes
import org.apache.spark.sql.Dataset

object LiberalFractures  extends OutcomeTransformer with FractureCodes{

  override val outcomeName = "Liberal"

  def transform(events: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {
    import events.sqlContext.implicits._

    events
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }

}
