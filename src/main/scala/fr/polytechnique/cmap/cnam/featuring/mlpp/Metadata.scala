package fr.polytechnique.cmap.cnam.featuring.mlpp

case class Metadata(
  rows: Int,
  columns: Int,
  patients: Int,
  buckets: Int,
  bucketSize: Int,
  molecules: Int,
  lags: Int
)
