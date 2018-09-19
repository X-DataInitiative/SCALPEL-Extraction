package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object MoleculeLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugConfig]): List[Event[Drug]] = {

    if(isInFamily(families, purchase.CIP13)) {
      val molecules = purchase.molecules
        .split("_")
        .toList
      molecules.map(molecule => Drug(purchase.patientID, molecule, purchase.conditioning, purchase.eventDate))
    }
    else List.empty
  }
}