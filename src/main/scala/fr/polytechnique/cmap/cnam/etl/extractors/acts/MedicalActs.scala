package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql._

class MedicalActs(config: MedicalActsConfig) extends Serializable with MCOSourceInfo with DCIRSourceInfo {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {

    val dcirActs = new DCIRSourceExtractor().extract(
      sources.dcir.get,
      List(new DCIRMedicalActEventExtractor(config.dcirCodes))
    )

    val mcoActs : Dataset[Event[MedicalAct]] = new MCOSourceExtractor().extract(
      sources.mco.get, List(
        new MCOMedicalActEventExtractor(MCOCols.DP, config.mcoCIMCodes, McoCIM10Act),
        new MCOMedicalActEventExtractor(MCOCols.CCAM, config.mcoCCAMCodes, McoCCAMAct)
      )
    )

    lazy val mcoCEActs = McoCEMedicalActs.extract( // todo finish
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
