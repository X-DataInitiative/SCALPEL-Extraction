package fr.polytechnique.cmap.cnam.util.reporting

/**
  * Represents the reporting metadata for a single operation.
  * An operation can be any method that touches a DataFrame, including but not limited to: readers,
  *   extractors, transformers and filters.
  */
case class OperationMetadata(
    name: String,
    inputs: List[String],
    outputType: OperationType,
    outputPath: Option[String],
    populationPath: Option[String])
  extends JsonSerializable

