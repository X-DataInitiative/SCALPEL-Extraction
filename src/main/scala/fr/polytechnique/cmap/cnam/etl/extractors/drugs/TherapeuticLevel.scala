package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object TherapeuticLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugConfig]): List[Event[Drug]] = {
    val filteredFamilies = families
      .filter(family => isInFamily(List(family), purchase.CIP13))
      .map(_.name)
    filteredFamilies.map(family => Drug(purchase.patientID, family, purchase.conditioning, purchase.eventDate))
  }
}
