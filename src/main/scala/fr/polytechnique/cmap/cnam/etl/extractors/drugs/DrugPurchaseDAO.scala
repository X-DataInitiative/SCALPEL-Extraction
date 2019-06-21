package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.sql.Timestamp

case class DrugPurchaseDAO(
    patientID: String,
    CIP13: String,
    ATC5: String = "",
    eventDate: Timestamp,
    molecules: String = "",
    conditioning: Int)

