package fr.polytechnique.cmap.cnam.study.bulk.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families.{Antihypertenseurs, AntihypertenseursHorsPathologies}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.PharmacologicalLevel
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugConfig, DrugExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object HTAExtractor {
  def extract(sources: Sources): Dataset[Event[Drug]] = {
    new DrugExtractor(DrugConfig(PharmacologicalLevel, List(AntihypertenseursHorsPathologies, Antihypertenseurs))).extract(sources, Set.empty)
  }
}
