package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{HospitalStay, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.{McoHospitalStaysExtractor, SsrHospitalStaysExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class HospitalStaysExtractor() {

  def extract(sources: Sources): Dataset[Event[HospitalStay]] = {

    val mcoHospitalStays = McoHospitalStaysExtractor.extract(sources, Set.empty)
    val ssrHospitalStays = SsrHospitalStaysExtractor.extract(sources, Set.empty)

    unionDatasets(mcoHospitalStays, ssrHospitalStays)
  }

}
