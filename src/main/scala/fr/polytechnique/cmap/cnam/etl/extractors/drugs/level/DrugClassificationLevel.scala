package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig

trait DrugClassificationLevel extends Serializable {

  def isInFamily(families: List[DrugClassConfig], row: Row): Boolean

  def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String]
}

object DrugClassificationLevel {

  def fromString(level: String): DrugClassificationLevel =
    level.toLowerCase match {
      case "therapeutic" => TherapeuticLevel
      case "pharmacological" => PharmacologicalLevel
      case "moleculecombination" => MoleculeCombinationLevel
      case "cip13" => Cip13Level
    }
}