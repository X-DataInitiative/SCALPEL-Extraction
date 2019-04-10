package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugConfig, NewDrugExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DrugsExtractor (drugConfig: DrugConfig){

  def extract(sources : Sources): Dataset[Event[Drug]] = {
    new NewDrugExtractor(drugConfig).extract(sources, Set.empty)
  }
}
