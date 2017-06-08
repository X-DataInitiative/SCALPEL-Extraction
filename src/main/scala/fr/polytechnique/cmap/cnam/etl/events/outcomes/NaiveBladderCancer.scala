package fr.polytechnique.cmap.cnam.etl.events.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.Diagnosis
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

object NaiveBladderCancer extends OutcomeTransformer {

  val name: String = "naive_bladder_cancer"
  val code: String = "C67"

  def transform(extracted: Dataset[Event[AnyEvent]]): Dataset[Event[Outcome]] = {
    import extracted.sqlContext.implicits._
    extracted
      .filter(event => Diagnosis.categories.contains(event.category) && event.value == code)
      //.dropDuplicates("patientId", "groupID")
      .map(event => Outcome(event.patientID, name, event.start))
  }
}