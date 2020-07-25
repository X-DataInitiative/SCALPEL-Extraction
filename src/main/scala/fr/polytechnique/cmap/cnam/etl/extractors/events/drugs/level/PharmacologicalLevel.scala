// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.DrugClassConfig

object PharmacologicalLevel extends DrugClassificationLevel {

  override def isInFamily(
    families: List[DrugClassConfig],
    row: Row): Boolean = families
    .flatMap(_.pharmacologicalClasses)
    .exists(family => family.isCorrect(row.getAs[String]("ATC5"), ""))

  override def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String] = {

    if (families.isEmpty) {
      val cip13 = row.getAs[String]("CIP13")
      Seq(cip13)
    }
    else {
      families
        .flatMap(_.pharmacologicalClasses)
        .filter(family => family.isCorrect(row.getAs[String]("ATC5"), ""))
        .map(_.name)
    }
  }

}
