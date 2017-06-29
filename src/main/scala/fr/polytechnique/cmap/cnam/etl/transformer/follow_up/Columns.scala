package fr.polytechnique.cmap.cnam.etl.transformer.follow_up

import fr.polytechnique.cmap.cnam.etl.events.Event


private[follow_up] object Columns {
  final val PatientID = FollowUp.Columns.PatientID
  final val Start = FollowUp.Columns.Start
  final val Stop = FollowUp.Columns.Stop
  final val EndReason = FollowUp.Columns.EndReason

  final val Category = Event.Columns.Category
  final val GroupID = Event.Columns.GroupID
  final val Value = Event.Columns.Value
  final val Weight = Event.Columns.Weight

  final val DeathDate = "deathDate"

  final val ObservationStart = "observationStart"
  final val ObservationEnd = "observationEnd"

  final val FollowUpStart = "followUpStart"
  final val FollowUpEnd = "followUpEnd"

  final val TrackLossDate = "trackloss"
  final val FirstTargetDiseaseDate = "firstTargetDisease"

}
