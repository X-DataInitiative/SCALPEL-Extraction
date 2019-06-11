package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers._
import fr.polytechnique.cmap.cnam.etl.transformers.TransformerConfig
import fr.polytechnique.cmap.cnam.etl.transformers.observation._

class FollowUpTransformerConfig(
    val patients: Option[Dataset[(Patient, Event[ObservationPeriod])]] = None,
    val dispensations: Option[Dataset[Event[Molecule]]] = None,
    val outcomes: Option[Dataset[Event[Outcome]]] = None,
    val tracklosses: Option[Dataset[Event[Trackloss]]] = None,
    val delayMonths: Int,
    val firstTargetDisease: Boolean,
    val outcomeName: Option[String]) extends TransformerConfig

object FollowUpTransformerConfig {

  def apply(
    patients: Option[Dataset[(Patient, Event[ObservationPeriod])]] = None,
    dispensations: Option[Dataset[Event[Molecule]]] = None,
    outcomes: Option[Dataset[Event[Outcome]]] = None,
    tracklosses: Option[Dataset[Event[Trackloss]]] = None,
    delayMonths: Int, firstTargetDisease: Boolean, outcomeName: Option[String]) =
    new FollowUpTransformerConfig(
      patients,
      dispensations,
      outcomes,
      tracklosses,
      delayMonths, firstTargetDisease, outcomeName)
}
