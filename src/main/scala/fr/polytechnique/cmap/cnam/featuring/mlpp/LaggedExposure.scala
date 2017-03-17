package fr.polytechnique.cmap.cnam.featuring.mlpp

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
    weight: Double,
    censoringBucket: Option[Int] = None // todo: remove this default and update tests
)
