package fr.polytechnique.cmap.cnam.study.pioglitazone

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.Diagnosis
import fr.polytechnique.cmap.cnam.etl.events.outcomes.{Outcome, OutcomeTransformer}
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=60093054
 */

object NaiveBladderCancer extends OutcomeTransformer with PioglitazoneStudyCodes {

  override val outcomeName: String = "naive_bladder_cancer"

   def transform(extracted: Dataset[Event[AnyEvent]]): Dataset[Event[Outcome]] = {
    import extracted.sqlContext.implicits._
    extracted
      .filter(event => Diagnosis.categories.contains(event.category) && event.value == primaryDiagCode)
      .dropDuplicates("patientId", "groupID")
      .map(event => Outcome(event.patientID, outcomeName, event.start))
  }
}
