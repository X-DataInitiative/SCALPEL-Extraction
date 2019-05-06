package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig

object MoleculeCombinationLevel extends DrugClassificationLevel {

  override def isInFamily(
    families: List[DrugClassConfig],
    row: Row): Boolean = families
    .exists(family => family.cip13Codes.contains(row.getAs[String]("CIP13")))

  override def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String] =
    Seq(row.getAs[String]("molecules"))
}
