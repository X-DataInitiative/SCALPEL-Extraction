package fr.polytechnique.cmap.cnam.study.fall.config

import pureconfig.ConfigReader
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.MedicalActsConfig
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.DiagnosesConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig
import fr.polytechnique.cmap.cnam.study.fall.fractures.BodySite
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.observation._
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up._
import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.DrugClassConfig

class FallConfigLoader extends ConfigLoader {

  //For reading DrugConfigClasses that are related to the Fall study
  implicit val drugConfigReader: ConfigReader[DrugClassConfig] = ConfigReader[String].map(
    family =>
      FallDrugClassConfig.familyFromString(family)
  )

  //For reading Body Sites
  implicit val bodySitesReader: ConfigReader[BodySite] = ConfigReader[String].map(
    site =>
      BodySite.fromString(site)
  )

  // ENTERING HACKVILLE
  implicit val patientReader: ConfigReader[Option[Dataset[Tuple2[Patient, Event[FollowUp]]]]] = ConfigReader[String].map(
    site => None
  )
  implicit val noneReader: ConfigReader[Option[Dataset[Event[Drug]]]] = ConfigReader[String].map(
    site => None
  )
  // EXITING HACKVILLE
}
