package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.EventsExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object MoleculePurchases extends EventsExtractor[Molecule] {

  def extract(
      config: ExtractionConfig,
      sources: Sources): Dataset[Event[Molecule]] = {

    DcirMoleculePurchases.extract(
      config, sources.dcir.get, sources.irPha.get, sources.dosages.get
    )
  }
}
