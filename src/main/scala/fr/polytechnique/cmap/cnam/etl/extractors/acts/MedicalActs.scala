package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val sqlCtx = sources.dcir.get.sqlContext
    import sqlCtx.implicits._

    val dcirActs = DcirMedicalActs.extract(sources.dcir.get, config.dcirCodes)

    val mcoActs = McoMedicalActs.extract(
      sources.pmsiMco.get,
      config.mcoCIMCodes,
      config.mcoCCAMCodes
    )

    val mcoCEActs = if (sources.pmsiMcoCE.isEmpty) {
      sources.dcir.get.sparkSession.emptyDataset[Event[MedicalAct]]
    }else {
      McoCEMedicalActs.extract(
        sources.pmsiMcoCE.get,
        config.mcoCECodes
      )
    }

    dcirActs.union(mcoActs).union(mcoCEActs)
  }
}
