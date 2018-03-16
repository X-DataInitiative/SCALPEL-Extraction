package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.Dataset

class MoleculePurchases(config: MoleculePurchasesConfig) {

  def extract(sources: Sources): Dataset[Event[Molecule]] = {
    DcirMoleculePurchases.extract(
      sources.dcir.get, sources.irPha.get, sources.dosages.get,
      config.drugClasses, config.maxBoxQuantity
    )
  }
}
