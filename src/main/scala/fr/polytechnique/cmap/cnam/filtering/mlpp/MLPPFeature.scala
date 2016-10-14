package fr.polytechnique.cmap.cnam.filtering.mlpp

case class MLPPFeature(
  patientID: String,
  patientIndex: Long,
  moleculeName: String,
  moleculeIndex: Long,
  bucketIndex: Int,
  lagIndex: Int,
  rowIndex: Long,
  colIndex: Long,
  value: Double)

object MLPPFeature {

  def fromLaggedExposure(e: LaggedExposure, bucketCount: Int, lagCount: Int): MLPPFeature = {

    val r = e.patientIDIndex * bucketCount + e.startBucket
    val c = e.moleculeIndex * lagCount + e.lag

    MLPPFeature(
      patientID = e.patientID,
      patientIndex = e.patientIDIndex,
      moleculeName = e.molecule,
      moleculeIndex = e.moleculeIndex,
      lagIndex = e.lag,
      bucketIndex = e.startBucket,
      rowIndex = r,
      colIndex = c,
      value = e.weight
    )
  }
}