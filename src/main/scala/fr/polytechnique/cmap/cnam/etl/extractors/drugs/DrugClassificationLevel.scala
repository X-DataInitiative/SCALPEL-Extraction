package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

trait DrugClassificationLevel extends Serializable {

  def isInFamily(families: List[DrugClassConfig], cip13: String): Boolean =  families
    .exists(family => family.cip13Codes.contains(cip13))

  def apply(purchase: Purchase, families: List[DrugClassConfig]): List[Event[Drug]]

  def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String]
}

object DrugClassificationLevel{

  def fromString(level: String) : DrugClassificationLevel =
    level match {
      case "Therapeutic" => TherapeuticLevel
      case "Pharmacological" => PharmacologicalLevel
      case "MoleculeCombination" => MoleculeCombinationLevel
      case "CIP13" => Cip13Level
  }
}