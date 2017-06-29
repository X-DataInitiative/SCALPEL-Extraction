package fr.polytechnique.cmap.cnam.etl.transformer.follow_up

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.Diagnosis
import fr.polytechnique.cmap.cnam.etl.events.molecules.Molecule
import fr.polytechnique.cmap.cnam.etl.events.tracklosses.Trackloss
import fr.polytechnique.cmap.cnam.etl.transformer.observation._
import fr.polytechnique.cmap.cnam.etl.transformer.follow_up.Columns._
import fr.polytechnique.cmap.cnam.util.RichDataFrames._
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._


class FollowUpTransformer(val delay: Int) {

  val outputColumns = List(
    col(PatientID),
    col(FollowUpStart).as(Start),
    col(FollowUpEnd).as(Stop),
    col(EndReason)
  )


  def transform(
      patients: Dataset[(Patient, ObservationPeriod)],
      dispensations: Dataset[Event[Molecule]],
      outcomes: Dataset[Event[Diagnosis]],
      tracklosses: Dataset[Event[Trackloss]])
    : Dataset[FollowUp] = {

    import FollowUpTransformer._

    val inputCols = Seq(
      col("Patient.patientID").as(PatientID),
      col("Patient.deathDate").as(DeathDate),
      col("ObservationPeriod.start").as(ObservationStart),
      col("ObservationPeriod.stop").as(ObservationEnd)
    )

    val events = dispensations.toDF
      .union(outcomes.toDF)
      .union(tracklosses.toDF)

    val input = renameTupleColumns(patients)
      .select(inputCols:_*)
      .join(events, Seq(PatientID))

    import input.sqlContext.implicits._

    input.repartition(col(PatientID))
      .withFollowUpStart(delay)
      .withTrackloss
      .withFollowUpEnd
      .na.drop("any", Seq(FollowUpStart, FollowUpEnd))
      .withEndReason
      .select(outputColumns: _*)
      .dropDuplicates(Seq(PatientID))
      .as[FollowUp]
  }
}

object FollowUpTransformer {

  implicit class FollowUpDataFrame(data: DataFrame) {

    def withFollowUpStart(followUpMonthsDelay: Int): DataFrame = {
      val window = Window.partitionBy(PatientID)

      val followUpStart = add_months(col(ObservationStart), followUpMonthsDelay).cast(TimestampType)
      val correctedFollowUpStart = when(followUpStart < col(ObservationEnd), followUpStart)

      data.withColumn(FollowUpStart, min(correctedFollowUpStart).over(window))
    }

    def withTrackloss: DataFrame = {
      val window = Window.partitionBy(PatientID)

      val firstCorrectTrackloss = min(
        when(col(Category) === Trackloss.category && (col(Start) > col(FollowUpStart)), col(Start))
      ).over(window)

      data.withColumn(TrackLossDate, firstCorrectTrackloss)
    }

    def withFollowUpEnd: DataFrame = {
      val window = Window.partitionBy(PatientID)

      val firstTargetDisease = min(
        when(col(Category) === "disease" && col(Value) === "targetDisease", col(Start)) // TODO need refacto
      ).over(window)

      data
        .withColumn(FirstTargetDiseaseDate, firstTargetDisease)
        .withColumn(FollowUpEnd,
          minColumn(
            col(DeathDate),
            col(FirstTargetDiseaseDate),
            col(TrackLossDate),
            col(ObservationEnd)
          )
        )
    }

    def withEndReason: DataFrame = {
      val endReason = when(
        col(FollowUpEnd) === col(DeathDate), "death"
      ).when(
        col(FollowUpEnd) === col(FirstTargetDiseaseDate), "disease"
      ).when(
        col(FollowUpEnd) === col(TrackLossDate), "trackloss"
      ).when(
        col(FollowUpEnd) === col(ObservationEnd), "observationEnd"
      )

      data.withColumn(EndReason, endReason)
    }
  }
}
