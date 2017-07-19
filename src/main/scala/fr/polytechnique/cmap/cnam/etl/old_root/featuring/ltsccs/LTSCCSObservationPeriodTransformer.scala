package fr.polytechnique.cmap.cnam.etl.old_root.featuring.ltsccs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.etl.old_root.ObservationPeriodTransformer
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._

// For LTSCCS, we define the observation start as the date of the first A10 medicine purchase and
//   the end as the minimum between the death date and the end of the study
object LTSCCSObservationPeriodTransformer extends ObservationPeriodTransformer {

  val window = Window.partitionBy("patientID")

  override def computeObservationStart(data: DataFrame): DataFrame =  {
    val correctedStart = when(
      lower(col("category")) === "molecule" && (col("start") >= StudyStart), col("start")
    )
    data.withColumn("observationStart", min(correctedStart).over(window).cast(TimestampType))
  }

  override def computeObservationEnd(data: DataFrame): DataFrame = {
    val firstCorrectTrackloss = min(
      when(col("category") === "trackloss" && (col("start") > col("observationStart")), col("start"))
    ).over(window)

    data
      .withColumn("globalTrackloss", firstCorrectTrackloss)
      .withColumn("observationEnd", minColumn(
        col("globalTrackloss"),
        col("deathDate"),
        lit(StudyEnd))
      )
      .drop("globalTrackloss")
  }
}
