package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors._
import fr.polytechnique.cmap.cnam.etl.extractors.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class ActsExtractor(config: MedicalActsConfig) {
  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {

    val dcirMedicalAct = new DCIRSourceExtractor().extract(sources.dcir.get, List(
                             new DCIRMedicalActEventExtractor(config.dcirCodes)))

    val mcoCEMedicalActs = new McoCEMedicalActEventExtractor()
                               .extract(sources.mcoCe.get, config.mcoCECodes)

    unionDatasets(dcirMedicalAct, mcoCEMedicalActs)
  }
}
