package fr.polytechnique.cmap.cnam.etl.events.acts

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.{Event, EventsExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class MedicalActs extends EventsExtractor[MedicalAct] {

  override def extract(config: ExtractionConfig, sources: Sources): Dataset[Event[MedicalAct]] = {

    val dcirActs = DcirMedicalActs.extract(sources.dcir.get, config.dcirMedicalActCodes)

    val mcoActs = McoMedicalActs.extract(
      sources.pmsiMco.get,
      config.mcoCIM10MedicalActCodes,
      config.mcoCCAMMedicalActCodes
    )

    dcirActs.union(mcoActs)
  }
}
