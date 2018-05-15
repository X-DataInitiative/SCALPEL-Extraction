package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.rosiglitazone.RosiglitazoneStudyCodes

object Infarctus extends OutcomesTransformer{

  override val outcomeName: String = "infarctus"

  def transform(extracted: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {
    import extracted.sqlContext.implicits._
    extracted
      .filter(e => RosiglitazoneStudyCodes.diagCodeInfarct.contains(e.value))
      .dropDuplicates(Event.Columns.PatientID, Event.Columns.GroupID)
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }

}

