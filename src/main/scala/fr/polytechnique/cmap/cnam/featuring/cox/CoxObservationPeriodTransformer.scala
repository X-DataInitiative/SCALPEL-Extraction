package fr.polytechnique.cmap.cnam.featuring.cox

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.etl.old_root.ObservationPeriodTransformer

// For Cox, the observation start is the date of the first A10 medicine purchase and the end is
//   the end of the study
object CoxObservationPeriodTransformer extends ObservationPeriodTransformer {

  override def computeObservationStart(data: DataFrame): DataFrame =  {
    val window = Window.partitionBy("patientID")
    val correctedStart = when(
      lower(col("category")) === "molecule" && (col("start") >= StudyStart), col("start")
    )
    data.withColumn("observationStart", min(correctedStart).over(window).cast(TimestampType))
  }

  override def computeObservationEnd(data: DataFrame): DataFrame = {
    data.withColumn("observationEnd", lit(StudyEnd))
  }
}
