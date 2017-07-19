package fr.polytechnique.cmap.cnam.etl.old_root.featuring.mlpp

case class Metadata(
  rows: Int,
  columns: Int,
  patients: Int,
  buckets: Int,
  bucketSize: Int,
  molecules: Int,
  lags: Int
)
