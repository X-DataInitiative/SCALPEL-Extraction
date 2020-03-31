// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugConfig, DrugExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families.Antihypertenseurs
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.TherapeuticLevel
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object HTAExtractor {
  def extract(sources: Sources): Dataset[Event[Drug]] = {
    new DrugExtractor(DrugConfig(TherapeuticLevel, List(Antihypertenseurs))).extract(sources)
  }
}
