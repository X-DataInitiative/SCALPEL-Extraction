// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.BaseExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class ActsExtractor(config: MedicalActsConfig) {
  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirMedicalAct = DcirMedicalActExtractor(BaseExtractorCodes(config.dcirCodes)).extract(sources)
      .filter(act => act.groupID != DcirAct.groupID.Unknown) // filter out unknown source acts
      .filter(act => act.groupID != DcirAct.groupID.PublicAmbulatory) //filter out public amb
    val mcoCEMedicalActs = McoCeCcamActExtractor(BaseExtractorCodes(config.mcoCECodes)).extract(sources)
    val mcoMedicalActs = McoCcamActExtractor(BaseExtractorCodes(config.mcoCCAMCodes)).extract(sources)

    unionDatasets(dcirMedicalAct, mcoCEMedicalActs, mcoMedicalActs)
  }

}
