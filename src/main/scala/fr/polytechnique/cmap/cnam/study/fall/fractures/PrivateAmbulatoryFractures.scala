// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

object PrivateAmbulatoryFractures extends OutcomesTransformer with FractureCodes {

  override val outcomeName: String = "private_ambulatory_fall"

  def transform(events: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {
    import events.sqlContext.implicits._

    events
      .filter(isPrivateAmbulatory _)
      .filter(containsNonHospitalizedCcam _)
      .map(
        event => {
          val fractureSite = BodySite.getSiteFromCode(event.value, BodySites.sites, CodeType.CCAM)
          Outcome(event.patientID, fractureSite, outcomeName, 1.0D, event.start)
        }
      )
  }

  def isPrivateAmbulatory(event: Event[MedicalAct]): Boolean = {
    event.groupID == DcirAct.groupID.PrivateAmbulatory
  }

  def containsNonHospitalizedCcam(event: Event[MedicalAct]): Boolean = {
    NonHospitalizedFracturesCcam.exists {
      code => event.value.startsWith(code)
    }
  }

}
