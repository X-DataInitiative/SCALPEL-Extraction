package fr.polytechnique.cmap.cnam.etl.loader.mlpp

case class LaggedExposure(
    patientID: String,
    patientIDIndex: Int,
    gender: Int,
    age: Int,
    diseaseBucket: Option[Int],
    molecule: String,
    moleculeIndex: Int,
    startBucket: Int,
    endBucket: Int,
    lag: Int,
    weight: Double
)
