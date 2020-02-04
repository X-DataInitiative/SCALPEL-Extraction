// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.drugprescription

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, DrugPrescription, Event}

class DrugPrescriptionTransformer extends Serializable {
  /**
    * Transform DrugPurchases Events to DrugPrescription Events.
    * @param drugs [[Dataset]][[Event]][[Drug]]
    * @return [[Dataset]][[Event]][[DrugPrescription]]
    */
  def transform(drugs: Dataset[Event[Drug]]): Dataset[Event[DrugPrescription]] = {

    val sqlCtx = drugs.sqlContext
    import sqlCtx.implicits._
    drugs
      .groupByKey(drug => (drug.groupID, drug.patientID, drug.start))
      .mapGroups((_, drugs) => fromDrugs(drugs.toList))
      .distinct()
  }

  /**
    * Combines [[Drug]] [[Event]] to form an [[Event]] of type [[DrugPrescription]].
    * WARNING: Drug Events must share the same patientID, groupID and start.
    * @param drugs Events to be combined. Must share the same patientID, groupID and start.
    * @return DrugPrescription Event which value is concatenation of the values of the passed Drugs.
    */
  def fromDrugs(drugs: List[Event[Drug]]): Event[DrugPrescription] = {
    val first = drugs.head
    val value = drugs
      .map(_.value)
      .sorted
      .reduce((l, r) => l.concat("_").concat(r))
    DrugPrescription(first.patientID, value, first.weight, first.groupID, first.start)
  }
}
