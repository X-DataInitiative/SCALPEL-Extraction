package fr.polytechnique.cmap.cnam.etl.old_root.featuring.mlpp

case class MLPPFeature(
  patientID: String,
  patientIndex: Int,
  moleculeName: String,
  moleculeIndex: Int,
  bucketIndex: Int,
  lagIndex: Int,
  rowIndex: Int,
  colIndex: Int,
  value: Double)

object MLPPFeature {

  def fromLaggedExposure(e: LaggedExposure, bucketCount: Int, lagCount: Int): MLPPFeature = {

    val r: Int = e.patientIDIndex * bucketCount + e.startBucket
    val c: Short = (e.moleculeIndex * lagCount + e.lag).toShort

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