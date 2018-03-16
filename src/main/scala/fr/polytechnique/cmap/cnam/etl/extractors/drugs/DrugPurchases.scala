package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.sources.OldSources


trait DrugPurchases {

  val drugCodes: Map[String, Set[String]]

  def extract(sources : OldSources): Dataset[Event[Drug]]
}
