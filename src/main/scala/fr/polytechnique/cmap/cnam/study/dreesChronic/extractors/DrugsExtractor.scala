package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.{DrugConfig, DrugExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources


class DrugsExtractor (drugConfig: DrugConfig){

  def extract(sources : Sources): Dataset[Event[Drug]] = {
    new DrugExtractor(drugConfig).extract(sources)
  }
}
