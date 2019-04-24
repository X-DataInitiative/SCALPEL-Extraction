package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugClassConfig, Purchase}

object Cip13Level extends DrugClassificationLevel{
  override def apply(
    purchase: Purchase,
    families: List[DrugClassConfig]): List[Event[Drug]] = {
    if(isInFamily(families, purchase.CIP13)) {
      List(Drug(purchase.patientID, purchase.CIP13, purchase.conditioning, purchase.eventDate))
    }
    else List.empty
  }

  override def getClassification(families: Seq[DrugClassConfig])
    (row: Row): Seq[String] = Seq(row.getAs[String]("CIP13"))
}
