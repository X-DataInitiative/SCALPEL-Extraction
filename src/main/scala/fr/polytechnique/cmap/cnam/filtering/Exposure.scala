package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

case class Exposure(
    patientID: String,
    gender: Int,
    birthDate: Timestamp,
    deathDate: Option[Timestamp],
    molecule: String,
    start: Timestamp,
    end: Timestamp)
