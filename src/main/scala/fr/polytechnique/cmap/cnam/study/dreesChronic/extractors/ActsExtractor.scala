package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class ActsExtractor(config: MedicalActsConfig) {
  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirMedicalAct = DcirMedicalActExtractor.extract(sources, config.dcirCodes.toSet)
      .filter(act => act.groupID != DcirAct.groupID.Unknown) // filter out unknown source acts
      .filter(act => act.groupID != DcirAct.groupID.PublicAmbulatory) //filter out public amb
    val mcoCEMedicalActs = McoCeActExtractor.extract(sources, config.mcoCECodes.toSet)
    val mcoMedicalActs = McoCcamActExtractor.extract(sources, config.mcoCCAMCodes.toSet)
    val ssrMedicalActs = SsrCcamActExtractor.extract(sources, config.ssrCCAMCodes.toSet)

    unionDatasets(dcirMedicalAct, mcoCEMedicalActs, mcoMedicalActs, ssrMedicalActs)
  }

}
