// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.DrugClassConfig

object TherapeuticLevel extends DrugClassificationLevel {

  override def isInFamily(
    families: List[DrugClassConfig],
    row: Row): Boolean = families
    .exists(family => family.cip13Codes.contains(row.getAs[String]("CIP13")))

  override def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String] = {
    val cip13 = row.getAs[String]("CIP13")
    if (families.isEmpty) {
      Seq(cip13)
    }
    else {
      families.filter(family => family.cip13Codes.contains(row.getAs[String]("CIP13"))).map(_.name)
    }
  }
}

