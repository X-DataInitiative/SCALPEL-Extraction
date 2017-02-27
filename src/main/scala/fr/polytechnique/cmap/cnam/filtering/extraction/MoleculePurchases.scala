package fr.polytechnique.cmap.cnam.filtering.extraction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.filtering.events._

object MoleculePurchases extends EventsExtractor[Molecule] {

  def extract(
      config: ExtractionConfig,
      sources: Sources): Dataset[Event[Molecule]] = {

    DcirMoleculePurchases.extract(
      config, sources.dcir.get, sources.irPha.get, sources.dosages.get
    )
  }
}
