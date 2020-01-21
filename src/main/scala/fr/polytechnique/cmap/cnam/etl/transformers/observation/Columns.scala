package fr.polytechnique.cmap.cnam.etl.transformers.observation

import fr.polytechnique.cmap.cnam.etl.events.Event

/** Private object for the package [[fr.polytechnique.cmap.cnam.etl.transformers.observation]]
  * to retrieve the columns of event object of type ObservationPeriod.
  * */
private[observation] object Columns {

  final val PatientID = Event.Columns.PatientID
  final val Start = Event.Columns.Start
  final val End = Event.Columns.End
  final val EndReason = Event.Columns.Value

  final val Category = Event.Columns.Category
  final val GroupID = Event.Columns.GroupID
  final val Value = Event.Columns.Value
  final val Weight = Event.Columns.Weight

  final val DeathDate = "deathDate"

  final val ObservationStart = "observationStart"
  final val ObservationEnd = "observationEnd"

  final val FollowUpStart = "followUpStart"
  final val FollowUpEnd = "followUpEnd"

  final val TracklossDate = "trackloss"
  final val FirstTargetDiseaseDate = "firstTargetDisease"

}
