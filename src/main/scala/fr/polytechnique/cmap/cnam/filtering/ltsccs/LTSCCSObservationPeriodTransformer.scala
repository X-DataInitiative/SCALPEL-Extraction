package fr.polytechnique.cmap.cnam.filtering.ltsccs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.filtering.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.utilities.ColumnUtilities._

// For LTSCCS, we define the observation start as the date of the first A10 medicine purchase and
//   the end as the minimum between the death date and the end of the study
object LTSCCSObservationPeriodTransformer extends ObservationPeriodTransformer {

  override def computeObservationStart(data: DataFrame): DataFrame =  {
    val window = Window.partitionBy("patientID")
    val correctedStart = when(
      lower(col("category")) === "molecule" && (col("start") >= StudyStart), col("start")
    )
    data.withColumn("observationStart", min(correctedStart).over(window).cast(TimestampType))
  }

  override def computeObservationEnd(data: DataFrame): DataFrame = {
    data.withColumn("observationEnd", minColumn(col("deathDate"), lit(StudyEnd)))
  }
}
