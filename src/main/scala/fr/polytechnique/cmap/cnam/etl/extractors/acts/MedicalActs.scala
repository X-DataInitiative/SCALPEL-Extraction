package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirActs = DcirMedicalActs.extract(sources.dcir.get, config.dcirCodes)

    val mcoActs = McoMedicalActs(
      config.mcoCIMCodes,
      config.mcoCCAMCodes
    ).extract[MedicalAct](
      sources.mco.get
    )

    lazy val mcoCEActs = McoCEMedicalActs.extract(
      sources.mcoCe.get,
      config.mcoCECodes
    )

    if (sources.mcoCe.isEmpty) {
      unionDatasets(dcirActs, mcoActs)
    }
    else {
      unionDatasets(dcirActs, mcoActs, mcoCEActs)
    }
  }
}
