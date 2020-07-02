package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors


import fr.polytechnique.cmap.cnam.etl.events.{Event, PractitionerClaimSpeciality}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.prestations._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class PractitionnerClaimSpecialityExtractor(config: PractitionerClaimSpecialityConfig) {

  def extract(sources: Sources): Dataset[Event[PractitionerClaimSpeciality]] = {
    val nonMedicalSpeciality = NonMedicalPractitionerClaimExtractor(SimpleExtractorCodes(config.nonMedicalSpeCodes)).extract(sources)
    val medicalSpeciality = MedicalPractitionerClaimExtractor(SimpleExtractorCodes(config.medicalSpeCodes)).extract(sources)
    val mcoCeFbstcSpecialty = McoCeFbstcSpecialtyExtractor(SimpleExtractorCodes(config.medicalSpeCodes)).extract(sources)
    val mcoCeFcstcSpecialty = McoCeFcstcSpecialtyExtractor(SimpleExtractorCodes(config.medicalSpeCodes)).extract(sources)

    unionDatasets(
      nonMedicalSpeciality,
      medicalSpeciality,
      mcoCeFbstcSpecialty,
      mcoCeFcstcSpecialty
    )
  }
}
