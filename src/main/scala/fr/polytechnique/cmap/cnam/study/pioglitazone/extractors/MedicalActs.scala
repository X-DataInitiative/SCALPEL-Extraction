// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.pioglitazone.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.BaseExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirActs = DcirMedicalActExtractor(BaseExtractorCodes(config.dcirCodes)).extract(sources)
    val ccamActs = McoCcamActExtractor(BaseExtractorCodes(config.mcoCCAMCodes)).extract(sources)
    //val cimActs = McoCimMedicalActExtractor(BaseExtractorCodes(config.mcoCIMCodes)).extract(sources)

    unionDatasets(dcirActs, ccamActs) //, cimActs
  }
}
