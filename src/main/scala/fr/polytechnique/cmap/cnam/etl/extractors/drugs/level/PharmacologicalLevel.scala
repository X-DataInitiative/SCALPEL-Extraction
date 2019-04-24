package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugClassConfig, Purchase}

object PharmacologicalLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugClassConfig]): List[Event[Drug]] = {

    val filteredFamilies = families
      .flatMap(_.pharmacologicalClasses)
      .filter(family => family.isCorrect(purchase.ATC5, ""))
      .map(_.name)
    filteredFamilies.map(pharmaClass =>
      Drug(purchase.patientID, pharmaClass, purchase.conditioning, purchase.eventDate))
  }

  override def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String] = {
    val cip13 = row.getAs[String]("CIP13")
    if (families.isEmpty) {
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
