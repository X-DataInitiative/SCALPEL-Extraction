package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object PharmacologicalLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugClassConfig]): List[Event[Drug]] = {

    val filteredFamilies = families
      .flatMap(_.pharmacologicalClasses)
      .filter(family => family.isCorrect(purchase.ATC5, ""))
      .map(_.name)
    filteredFamilies.map(pharmaClass =>
      Drug(purchase.patientID, pharmaClass, purchase.conditioning, purchase.eventDate))
  }

}
