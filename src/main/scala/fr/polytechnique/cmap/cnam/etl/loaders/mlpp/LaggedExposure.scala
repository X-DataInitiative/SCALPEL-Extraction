package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

case class LaggedExposure(
  patientID: String,
  patientIDIndex: Int,
  gender: Int,
  age: Int,
  exposureType: String,
  exposureTypeIndex: Int,
  startBucket: Int,
  endBucket: Int,
  lag: Int,
  weight: Double)
