package fr.polytechnique.cmap.cnam.filtering
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

trait TargetDiseaseTransformer extends Transformer[Event] {

  final val DiseaseCode = "C67"

  val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("bladderCancer").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )
}

object TargetDiseaseTransformer extends TargetDiseaseTransformer {
  override def transform(sources: Sources): Dataset[Event] = {
    BladderCancerTransformer.transform(sources)
  }
}
