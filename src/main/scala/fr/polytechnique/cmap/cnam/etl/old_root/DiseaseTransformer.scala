package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

/**
  * This trait contains the skeleton of the output events and the target disease code
  */
trait DiseaseTransformer extends Transformer[Event] {
  final val DiseaseCode: String = FilteringConfig.diseaseCode

  protected val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit(DiseaseCode).as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

}

/**
  * This transformer looks for CIM10 code starting with C67 in IR_IMB_R and PMSI expected.MCO
 */
object DiseaseTransformer extends DiseaseTransformer {

  override def transform(sources: Sources): Dataset[Event] = {
    val transformers = List(
      ImbDiseaseTransformer,
      McoDiseaseTransformer)

    transformers.map(_.transform(sources)).reduce(_.union(_))
  }
}