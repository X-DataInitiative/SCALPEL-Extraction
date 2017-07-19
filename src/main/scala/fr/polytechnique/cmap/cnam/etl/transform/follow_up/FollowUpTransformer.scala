package fr.polytechnique.cmap.cnam.etl.transform.follow_up


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transform.observation._
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._
import fr.polytechnique.cmap.cnam.util.RichDataFrames._


class FollowUpTransformer(delay: Int, firstTargetDisease: Boolean, outcomeName: Option[String]) {
  import Columns._

  val outputColumns = List(
    col(PatientID),
    col(FollowUpStart).as(Start),
    col(FollowUpEnd).as(Stop),
    col(EndReason)
  )

  val followupEndModelCandidates = if(firstTargetDisease)
    List(col(TracklossDate), col(DeathDate), col(ObservationEnd), col(FirstTargetDiseaseDate))
  else
    List(col(TracklossDate), col(DeathDate), col(ObservationEnd))

  def transform(
      patients: Dataset[(Patient, ObservationPeriod)],
      dispensations: Dataset[Event[Molecule]],
      outcomes: Dataset[Event[Outcome]],
      tracklosses: Dataset[Event[Trackloss]]): Dataset[FollowUp] = {

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

    val window = Window.partitionBy(PatientID)

    val firstTargetDiseaseDate = min(
        when(col(Category) === Outcome.category && col(Value).contains(outcomeName.getOrElse(None)), col(Start))
    ).over(window)

    import input.sqlContext.implicits._

    input.repartition(col(PatientID))
      .withFollowUpStart(delay)
      .withTrackloss
      .withColumn(FirstTargetDiseaseDate, firstTargetDiseaseDate)
      .withColumn(FollowUpEnd,  minColumn(followupEndModelCandidates:_*))
      .na.drop("any", Seq(FollowUpStart, FollowUpEnd))
      .withEndReason
      .select(outputColumns: _*)
      .dropDuplicates(Seq(PatientID))
      .as[FollowUp]
  }


}

object FollowUpTransformer {
  import Columns._

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

      data.withColumn(TracklossDate, firstCorrectTrackloss)
    }

    def withEndReason: DataFrame = {
      val endReason = when(
        col(FollowUpEnd) === col(DeathDate), EndReasons.Death.toString
      ).when(
        col(FollowUpEnd) === col(FirstTargetDiseaseDate), EndReasons.Disease.toString
      ).when(
        col(FollowUpEnd) === col(TracklossDate), EndReasons.Trackloss.toString
      ).when(
        col(FollowUpEnd) === col(ObservationEnd), EndReasons.ObservationEnd.toString
      )

      data.withColumn(EndReason, endReason)
    }
  }
}
