package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.{DataFrame, Dataset}

object Acts {
  def extract(
               config: ExtractionConfig,
               sources: Sources
             ): Dataset[Event[MedicalAct]] = {

    unionDatasets(
      DcirActs.extract(config, sources.dcir.get),
      McoActs.extract(config, sources.pmsiMco.get)
    )
  }
}
