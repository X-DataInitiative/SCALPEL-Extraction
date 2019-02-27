package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}

object TherapeuticLevel extends DrugClassificationLevel {

  override def apply(purchase: Purchase, families: List[DrugClassConfig]): List[Event[Drug]] = {
    val filteredFamilies = families
      .filter(family => isInFamily(List(family), purchase.CIP13))
      .map(_.name)
    filteredFamilies.map(family => Drug(purchase.patientID, family, purchase.conditioning, purchase.eventDate))
  }

  override def getClassification(families: Seq[DrugClassConfig])(row: Row): Seq[String] = {
    val cip13 = row.getAs[String]("CIP13")
    if (families.isEmpty) {
      Seq(cip13)
    }
    else {
      families.filter(family => isInFamily(List(family), cip13)).map(_.name)
    }
  }
}
