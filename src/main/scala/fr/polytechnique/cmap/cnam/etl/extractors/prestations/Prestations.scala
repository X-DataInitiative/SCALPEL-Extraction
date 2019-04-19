package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import fr.polytechnique.cmap.cnam.etl.events.{PrestationSpeciality, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.Dataset

class Prestations(config: PrestationsConfig) {
  def extract(sources: Sources): Dataset[Event[PrestationSpeciality]] = {

    val dcirSpeciality = DcirPrestations(
      config.medicalSpeCodes,
      config.nonMedicalSpeCodes).extract(sources.dcir.get)

    dcirSpeciality.distinct()
  }
}
