package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

trait DrugClassificationLevel extends Serializable {
  def isInFamily(families: List[DrugClassConfig], cip13: String): Boolean =  families
    .exists(family => family.cip13Codes.contains(cip13))
  def apply(purchase: PurchaseDAO, families: List[DrugClassConfig]): List[Event[Drug]]
}

object DrugClassificationLevel{
  def fromString(level: String) : DrugClassificationLevel =
    level match {
      case "Therapeutic" => TherapeuticLevel
      case "Pharmacological" => PharmacologicalLevel
      case "MoleculeCombination" => MoleculeCombinationLevel
  }
}

object TherapeuticLevel extends DrugClassificationLevel {
  override def apply(purchase: PurchaseDAO, families: List[DrugClassConfig]): List[Event[Drug]] = {
    val filteredFamilies = families
      .filter(family => isInFamily(List(family), purchase.CIP13))
      .map(_.name)
    filteredFamilies.map(family => Drug(purchase.patientID, family, purchase.conditioning, purchase.eventDate))
  }
}

object MoleculeLevel extends DrugClassificationLevel {
  override def apply(purchase: PurchaseDAO, families: List[DrugClassConfig]): List[Event[Drug]] = {
    if(isInFamily(families, purchase.CIP13)) {
      val molecules = purchase.molecules
        .split("_")
        .toList
      molecules.map(molecule => Drug(purchase.patientID, molecule, purchase.conditioning, purchase.eventDate))
    }
    else List.empty
  }
}

object MoleculeCombinationLevel extends DrugClassificationLevel {
  override def apply(purchase: PurchaseDAO, families: List[DrugClassConfig]): List[Event[Drug]] = {
    if(isInFamily(families, purchase.CIP13))
      List(Drug(purchase.patientID, purchase.molecules, purchase.conditioning, purchase.eventDate))
    else List.empty
  }
}

object PharmacologicalLevel extends DrugClassificationLevel {
  override def apply(purchase: PurchaseDAO, families: List[DrugClassConfig]): List[Event[Drug]] = {
    val filteredFamilies = families
      .flatMap(_.pharmacologicalClasses)
      .filter(family => family.isCorrect(purchase.ATC5, ""))
      .map(_.name)
    filteredFamilies.map(pharmaClass =>
      Drug(purchase.patientID, pharmaClass, purchase.conditioning, purchase.eventDate))
  }
}
