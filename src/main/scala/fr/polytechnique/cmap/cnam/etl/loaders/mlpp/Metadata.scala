package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

case class Metadata(
  rows: Int,
  columns: Int,
  patients: Int,
  buckets: Int,
  bucketSize: Int,
  exposureType: Int,
  lags: Int)
