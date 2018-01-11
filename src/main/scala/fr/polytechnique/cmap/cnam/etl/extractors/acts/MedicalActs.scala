package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}

class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirActs = DcirMedicalActs.extract(sources.dcir.get, config.dcirCodes)

    val mcoActs = McoMedicalActs.extract(
      sources.pmsiMco.get,
      config.mcoCIMCodes,
      config.mcoCCAMCodes
    )

    lazy val mcoCEActs = McoCEMedicalActs.extract(
      sources.pmsiMcoCE.get,
      config.mcoCECodes
    )

    if (sources.pmsiMcoCE.isEmpty) {
      unionDatasets(dcirActs, mcoActs)
    }
    else {
      unionDatasets(dcirActs, mcoActs, mcoCEActs)
    }
  }
}
