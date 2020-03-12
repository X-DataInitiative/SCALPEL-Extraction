package fr.polytechnique.cmap.cnam.study.bulkDrees.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.{HadHospitalStaysExtractor, McoHospitalStaysExtractor, McoceEmergenciesExtractor, SsrHospitalStaysExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object PmsiHospitalStaysExtractor {
  def extract(sources: Sources): Dataset[Event[HospitalStay]] = {
    val mco = McoHospitalStaysExtractor.extract(sources, Set.empty[String])
    val mcoce = McoceEmergenciesExtractor.extract(sources, Set.empty[String])
    val ssr = SsrHospitalStaysExtractor.extract(sources, Set.empty[String])
    val had = HadHospitalStaysExtractor.extract(sources, Set.empty[String])
    unionDatasets(mco, mcoce, ssr, had)
  }
}
