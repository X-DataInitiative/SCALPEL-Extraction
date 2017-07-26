package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.Dataset

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomeTransformer


object PublicAmbulatoryFall extends  OutcomeTransformer with FallStudyCodes {
  override val outcomeName: String = "public_ambulatory_fall"


  def isPublicAmbulatory(event: Event[MedicalAct]): Boolean = {
    event.category == McoCEAct.category
  }

  def isCorrectCamCode(event: Event[MedicalAct]): Boolean = {
    GenericCCAMCodes.map(event.value.startsWith).exists(identity)
  }

  def transform(events: Dataset[Event[MedicalAct]]): Dataset[Event[Outcome]] = {
    import events.sqlContext.implicits._

    events
      .filter(isPublicAmbulatory _)
      .filter(isCorrectCamCode _)
      .map(event =>
        Outcome(event.patientID, outcomeName, event.start))
  }
}
