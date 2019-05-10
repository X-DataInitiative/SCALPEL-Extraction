package fr.polytechnique.cmap.cnam.util.reporting

/**
  * Represents the reporting metadata to be generated for Main jobs
  */
case class MainMetadata(
  className: String,
  startTimestamp: java.util.Date,
  endTimestamp: java.util.Date,
  operations: List[OperationMetadata])
  extends JsonSerializable
