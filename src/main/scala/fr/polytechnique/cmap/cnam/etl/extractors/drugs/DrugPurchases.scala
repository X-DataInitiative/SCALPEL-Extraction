package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.Dataset


trait DrugPurchases {

  val drugCodes: Map[String, Set[String]]

  def extract(sources : Sources): Dataset[Event[Drug]]
}
