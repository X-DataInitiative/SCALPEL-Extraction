package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object PharmacologicalLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugConfig]): List[Event[Drug]] = {

    List(Drug(purchase.patientID, purchase.CIP13, 0, purchase.eventDate))

  }

}
