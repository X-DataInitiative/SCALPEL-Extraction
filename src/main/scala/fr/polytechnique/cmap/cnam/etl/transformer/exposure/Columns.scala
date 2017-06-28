package fr.polytechnique.cmap.cnam.etl.transformer.exposure

import fr.polytechnique.cmap.cnam.etl.events.Event


object Columns {
  final val PatientID = Event.Columns.PatientID
  final val Category = Event.Columns.Category
  final val GroupID = Event.Columns.GroupID
  final val Value = Event.Columns.Value
  final val Weight = Event.Columns.Weight
  final val Start = Event.Columns.Start
  final val End = Event.Columns.End

  final val ExposureStart = "exposureStart"
  final val ExposureEnd = "exposureEnd"

  final val Gender = "gender"
  final val BirthDate = "birthdate"
  final val DeathDate = "deathDate"

  final val FollowUpStart = "followUpStart"
  final val FollowUpEnd = "followUpEnd"

  final val TracklossDate = "tracklossDate"
}
