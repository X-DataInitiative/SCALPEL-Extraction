package fr.polytechnique.cmap.cnam.study.pioglitazone.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class MedicalActs(config: MedicalActsConfig) {

  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {
    val dcirActs = DcirMedicalActExtractor.extract(sources, config.dcirCodes.toSet)
    val ccamActs = McoCcamActExtractor.extract(sources, config.mcoCCAMCodes.toSet)
    val cimActs = McoCimMedicalActExtractor.extract(sources, config.mcoCIMCodes.toSet)

    unionDatasets(dcirActs, ccamActs, cimActs)
  }
}
