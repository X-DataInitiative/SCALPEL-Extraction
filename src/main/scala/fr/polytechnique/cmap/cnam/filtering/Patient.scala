package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

case class Patient(
    patientID: String,
    gender: Int,
    birthDate: Timestamp,
    deathDate: Option[Timestamp])
