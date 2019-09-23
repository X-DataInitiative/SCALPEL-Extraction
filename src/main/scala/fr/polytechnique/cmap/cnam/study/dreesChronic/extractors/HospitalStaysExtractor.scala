package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.{McoHospitalStaysExtractor, SsrHospitalStaysExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class HospitalStaysExtractor() {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mcoHospitalStays = McoHospitalStaysExtractor.extract(sources, Set.empty)
    val ssrHospitalStays = SsrHospitalStaysExtractor.extract(sources, Set.empty)

    unionDatasets(mcoHospitalStays, ssrHospitalStays)
  }

}
