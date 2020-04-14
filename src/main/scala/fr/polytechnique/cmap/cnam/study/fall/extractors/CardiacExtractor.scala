// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.families.Cardiac
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level.TherapeuticLevel
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.{DrugConfig, DrugExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object CardiacExtractor {
  def extract(sources: Sources): Dataset[Event[Drug]] = {
    new DrugExtractor(DrugConfig(TherapeuticLevel, List(Cardiac))).extract(sources)
  }
}
