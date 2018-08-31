package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object MoleculeCombinationLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugConfig]): List[Event[Drug]] = {

    if(isInFamily(families, purchase.CIP13))
      List(Drug(purchase.patientID, purchase.molecules, purchase.conditioning, purchase.eventDate))
    else List.empty

  }
}
