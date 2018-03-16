package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.OldSources

class MoleculePurchases(config: MoleculePurchasesConfig) {

  def extract(sources: OldSources): Dataset[Event[Molecule]] = {
    DcirMoleculePurchases.extract(
      sources.dcir.get, sources.irPha.get, sources.dosages.get,
      config.drugClasses, config.maxBoxQuantity
    )
  }
}
