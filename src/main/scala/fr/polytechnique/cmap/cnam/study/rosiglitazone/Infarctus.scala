package fr.polytechnique.cmap.cnam.study.rosiglitazone

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer
import org.apache.spark.sql.Dataset

object Infarctus extends OutcomeTransformer{

  override val outcomeName: String = "infarctus"

  def transform(extracted: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {
    import extracted.sqlContext.implicits._
    extracted
      .filter(e => RosiglitazoneStudyCodes.primaryDiagCodeInfract.contains(e.value))
      .dropDuplicates(Event.Columns.PatientID, Event.Columns.GroupID)
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }

}

