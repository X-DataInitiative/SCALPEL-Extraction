package fr.polytechnique.cmap.cnam.filtering.mlpp

case class LaggedExposure(
    patientID: String,
    patientIDIndex: Int,
    gender: Int,
    age: Int,
    molecule: String,
    moleculeIndex: Int,
    startBucket: Int,
    endBucket: Int,
    lag: Int,
    weight: Double
)
