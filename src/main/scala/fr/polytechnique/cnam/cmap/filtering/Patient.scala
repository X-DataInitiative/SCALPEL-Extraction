package fr.polytechnique.cnam.cmap.filtering

import java.sql.Timestamp

/**
  * @author Daniel de Paula
  */
case class Patient(
    patientID: String,
    gender: Int,
    birthDate: Timestamp,
    deathDate: Option[Timestamp])
