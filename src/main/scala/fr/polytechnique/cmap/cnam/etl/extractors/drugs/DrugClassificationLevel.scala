package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

trait DrugClassificationLevel extends Serializable {

  def isInFamily(families: List[DrugConfig], cip13: String): Boolean =  families
    .exists(family => family.cip13Codes.contains(cip13))
  def apply(purchase: Purchase, families: List[DrugConfig]): List[Event[Drug]]
}
