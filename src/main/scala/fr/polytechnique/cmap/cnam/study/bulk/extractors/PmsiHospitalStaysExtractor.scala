package fr.polytechnique.cmap.cnam.study.bulk.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.{McoHospitalStaysExtractor, McoceEmergenciesExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object PmsiHospitalStaysExtractor {
  def extract(sources: Sources): Dataset[Event[HospitalStay]] = {
    val mco = McoHospitalStaysExtractor.extract(sources, Set.empty[String])
    val mcoce = McoceEmergenciesExtractor.extract(sources, Set.empty[String])
    mco.union(mcoce)
  }
}
