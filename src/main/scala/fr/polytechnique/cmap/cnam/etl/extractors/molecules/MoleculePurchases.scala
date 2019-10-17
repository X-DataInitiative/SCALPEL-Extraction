// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class MoleculePurchases(config: MoleculePurchasesConfig) {

  def extract(sources: Sources): Dataset[Event[Molecule]] = {
    new DcirMoleculePurchases(config).extract(sources, config.drugClasses.toSet)
  }
}
