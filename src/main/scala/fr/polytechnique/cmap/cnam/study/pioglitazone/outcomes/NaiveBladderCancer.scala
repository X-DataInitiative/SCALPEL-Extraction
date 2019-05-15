package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, Outcome}
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.pioglitazone.PioglitazoneStudyCodes

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=60093054
 */

object NaiveBladderCancer extends OutcomesTransformer with PioglitazoneStudyCodes {

  override val outcomeName: String = "naive_bladder_cancer"

  def transform(extracted: Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {
    import extracted.sqlContext.implicits._
    extracted
      .filter(e => e.value == primaryDiagCode)
      .dropDuplicates(Event.Columns.PatientID, Event.Columns.GroupID)
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }
}
