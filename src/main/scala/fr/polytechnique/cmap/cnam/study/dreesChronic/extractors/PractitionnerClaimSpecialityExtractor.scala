package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors


import fr.polytechnique.cmap.cnam.etl.events.{PractitionerClaimSpeciality, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.prestations._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class PractitionnerClaimSpecialityExtractor(config: PractitionerClaimSpecialityConfig) {

  def extract(sources: Sources): Dataset[Event[PractitionerClaimSpeciality]] = {

    val nonMedicalSpeciality = NonMedicalPractitionerClaimExtractor(sources, config.nonMedicalSpeCodes.toSet)
    val medicalSpeciality = MedicalPractitionerClaimExtractor(sources, config.medicalSpeCodes.toSet)

    unionDatasets(nonMedicalSpeciality, medicalSpeciality)

  }
}
