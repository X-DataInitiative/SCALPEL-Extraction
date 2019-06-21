package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors._
import fr.polytechnique.cmap.cnam.etl.extractors.acts._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class ActsExtractor(config: MedicalActsConfig) extends Serializable with MCOSourceInfo with MCOCESourceInfo with DCIRSourceInfo {
  def extract(sources: Sources): Dataset[Event[MedicalAct]] = {

    val dcirMedicalAct_ : Dataset[Event[MedicalAct]]
      = new DCIRSourceExtractor(sources/*.dcir.get*/, List(
         new DCIRMedicalActEventExtractor(config.dcirCodes)))
          .extract() // THIS FAILS AS A SINGLE STATEMENT ?!?
    val dcirMedicalAct = dcirMedicalAct_
        .filter(act => act.groupID != DcirAct.groupID.Unknown) // filter out unknown source acts
        .filter(act => act.groupID != DcirAct.groupID.PublicAmbulatory) //filter out public amb

    val mcoMedicalActs : Dataset[Event[MedicalAct]]
      = new MCOSourceExtractor(sources, List(
         new MCOMedicalActEventExtractor(MCOCols.CCAM, config.mcoCECodes, McoCCAMAct)))
          .extract()

    val mcoCEMedicalActs : Dataset[Event[MedicalAct]]
      = new McoCEMedicalActEventExtractor(sources/*.mcoCe.get*/, config.mcoCECodes)
         .extract()

    unionDatasets(dcirMedicalAct, mcoCEMedicalActs, mcoMedicalActs)
  }
}
