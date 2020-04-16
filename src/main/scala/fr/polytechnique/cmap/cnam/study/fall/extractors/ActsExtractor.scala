// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.fractures.Surgery
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class ActsExtractor(config: MedicalActsConfig) extends Serializable {
  def extract(sources: Sources): (Dataset[Event[MedicalAct]], Dataset[Event[MedicalAct]]) = {
    val dcirMedicalAct = DcirMedicalActExtractor(SimpleExtractorCodes(config.dcirCodes)).extract(sources)
      .filter(act => act.groupID != DcirAct.groupID.Unknown) // filter out unknown source acts
      .filter(act => act.groupID != DcirAct.groupID.PublicAmbulatory) //filter out public amb
    val mcoCEMedicalActs = McoCeCcamActExtractor(SimpleExtractorCodes(config.mcoCECodes)).extract(sources)

    val surgeryCodes = Surgery.surgeryCodes
    val ccamCodes = config.mcoCCAMCodes
    val allMcoActs = McoCcamActExtractor(SimpleExtractorCodes(ccamCodes ++ surgeryCodes))
      .extract(sources)
      .cache()

    val fractureSurgeries = allMcoActs.filter(md => surgeryCodes.exists(code => code.startsWith(md.value)))
    val mcoMedicalActs = allMcoActs.filter(md => ccamCodes.exists(code => code.startsWith(md.value)))

    (unionDatasets(dcirMedicalAct, mcoCEMedicalActs, mcoMedicalActs), fractureSurgeries)
  }

}
