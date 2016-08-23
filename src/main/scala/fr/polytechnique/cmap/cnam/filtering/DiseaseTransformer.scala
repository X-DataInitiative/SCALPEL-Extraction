package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
  * This transformer looks for CIM10 code starting with C67 in IR_IMB_R
  */
trait DiseaseTransformer extends Transformer[Event] {
  final val DiseaseCode  = "C67"

  protected final val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit(DiseaseCode).as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

}

/**
  * While we don't have a TransformerPipeline.
  *
  * This feels weird though
 */
object DiseaseTransformer extends DiseaseTransformer {

  override def transform(sources: Sources): Dataset[Event] = {
    val transformers = List(
      ImbDiseaseTransformer,
      McoDiseaseTransformer)

    transformers.map(t => t.transform(sources))
      .reduce((dataset, otherDataset) => dataset.union(otherDataset))

  }
}