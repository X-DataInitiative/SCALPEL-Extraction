package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{PrestationSpeciality, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class Prestations(config: PrestationsConfig) {
  def extract(sources: Sources): Dataset[Event[PrestationSpeciality]] = {

    val dcirSpeciality = DcirPrestationsExtractor(
      config.medicalSpeCodes,
      config.nonMedicalSpeCodes).extract(sources.dcir.get)

    dcirSpeciality.distinct()
  }
}
