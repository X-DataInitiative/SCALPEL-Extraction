package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.Dataset

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer

/*
 * The rules for this Outcome definition can be found on the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/61282101/General+fractures+Fall+study
 */

object PrivateAmbulatoryFall extends OutcomeTransformer with FallStudyCodes{

  override val outcomeName: String = "private_ambulatory_fall"

  def isCorrectCamCode(event: Event[MedicalAct]): Boolean = {
    GenericCCAMCodes.map(event.value.startsWith).exists(identity)
  }

  def isPrivateAmbulatory(event: Event[MedicalAct]): Boolean = {
    event.groupID == DcirAct.groupID.PrivateAmbulatory
  }

  def transform(events: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {
    import events.sqlContext.implicits._

    events
      .filter(isPrivateAmbulatory _)
      .filter(isCorrectCamCode _)
      .map(event =>
      Outcome(event.patientID, outcomeName, event.start))
  }

}
