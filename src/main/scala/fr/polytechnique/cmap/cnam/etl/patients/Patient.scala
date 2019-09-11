package fr.polytechnique.cmap.cnam.etl.patients

import java.sql.Timestamp

case class Patient(
  patientID: String,
  gender: Int,
  birthDate: Timestamp,
  deathDate: Option[Timestamp])
