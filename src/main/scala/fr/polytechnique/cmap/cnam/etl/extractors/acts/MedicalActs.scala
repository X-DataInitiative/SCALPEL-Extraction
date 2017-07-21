package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {

    val dcirActs = DcirMedicalActs.extract(sources.dcir.get, config.dcirCodes)

    val mcoActs = McoMedicalActs.extract(
      sources.pmsiMco.get,
      config.mcoCIMCodes,
      config.mcoCCAMCodes
    )

    dcirActs.union(mcoActs)
  }
}
