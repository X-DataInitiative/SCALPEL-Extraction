package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.{HadHospitalStaysExtractor, McoHospitalStaysExtractor, SsrHospitalStaysExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class HospitalStaysExtractor() {

  def extract(sources: Sources): Dataset[Event[HospitalStay]] = {

    val mcoHospitalStays = McoHospitalStaysExtractor.extract(sources)
    val ssrHospitalStays = SsrHospitalStaysExtractor.extract(sources)
    val hadHospitalStays = HadHospitalStaysExtractor.extract(sources)
    unionDatasets(mcoHospitalStays, ssrHospitalStays, hadHospitalStays)
  }

}
